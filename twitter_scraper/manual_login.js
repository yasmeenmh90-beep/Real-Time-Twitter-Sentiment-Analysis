// manual_login.js
import { chromium } from 'playwright';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

// Where we'll save login artifacts
const STATE_PATH   = path.join(__dirname, 'state.json');
const COOKIES_PATH = path.join(__dirname, 'cookies.json');

// Use a persistent user data dir so the browser looks/behaves like a normal profile
const USER_DATA_DIR = path.join(__dirname, '.x-profile'); // will be created if missing

(async () => {
  // Launch persistent Chrome-like profile; add stealth args
  const context = await chromium.launchPersistentContext(USER_DATA_DIR, {
    headless: false,
    args: ['--disable-blink-features=AutomationControlled'],
    // If you have Google Chrome installed and want to use it:
    // channel: 'chrome',
    viewport: { width: 1280, height: 800 },
    // Let Playwright pick a modern UA automatically (often better than pinning an old one)
    // userAgent: undefined,
    locale: 'en-US',
  });

  // Hide webdriver flag as early as possible
  await context.addInitScript(() => {
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
  });

  const page = await context.newPage();

  const loginUrl = 'https://x.com/i/flow/login?lang=en';
  console.log('[OPEN] Go log in at', loginUrl);
  await page.goto(loginUrl, { waitUntil: 'domcontentloaded' });

  // Helpers
  const settle = async () => {
    try { await page.waitForLoadState('domcontentloaded', { timeout: 15000 }); } catch {}
    try { await page.waitForLoadState('networkidle',      { timeout: 15000 }); } catch {}
  };

  const tryConsent = async () => {
    try {
      const btn = page.locator(
        'button:has-text("Accept"), button:has-text("I agree"), [data-testid="confirmationSheetConfirm"]'
      ).first();
      if (await btn.isVisible({ timeout: 0 })) await btn.click().catch(() => {});
    } catch {}
  };

  const isLoggedIn = async () => {
    try {
      // side nav / home tab means we’re in
      const home = page
        .locator('[data-testid="SideNav_AccountSwitcher_Button"], [data-testid="AppTabBar_Home_Link"]')
        .first();
      if (await home.isVisible({ timeout: 0 })) return true;
    } catch {}
    try {
      const cookies = await context.cookies();
      const auth = cookies.find(c => c.name === 'auth_token' && c.value);
      return !!auth;
    } catch { return false; }
  };

  // Poll up to 4 minutes — plenty for 2FA/email/phone challenge
  const deadline = Date.now() + 4 * 60 * 1000;
  let bouncedHome = false;

  while (Date.now() < deadline) {
    await settle();
    await tryConsent();

    // If you see the Sign up page, click "Log in" at the bottom of that form
    // (X sometimes routes there first — that’s normal)
    try {
      const loginLink = page.locator('a:has-text("Log in")').first();
      if (await loginLink.isVisible({ timeout: 0 })) {
        await loginLink.click().catch(() => {});
        await settle();
      }
    } catch {}

    if (await isLoggedIn()) break;

    // “Something went wrong” → try /home once (X’s own fix path)
    try {
      const err = page.locator('text=Something went wrong').first();
      if (!bouncedHome && await err.isVisible({ timeout: 0 })) {
        bouncedHome = true;
        console.log('[INFO] Error wall → try /home');
        await page.goto('https://x.com/home', { waitUntil: 'domcontentloaded' });
        continue;
      }
    } catch {}

    await page.waitForTimeout(1000); // time to finish typing/2FA
  }

  if (!(await isLoggedIn())) {
    console.error('❌ Still not logged in. Finish username/password + any code/phone checks, then rerun.');
    await context.close();
    process.exit(1);
  }

  // Mirror cookies across x.com <-> twitter.com (helps later scraping)
  await settle();
  try {
    const ck = await context.cookies();
    const tw = ck.filter(c => /\.twitter\.com$/i.test(c.domain || ''));
    const x  = ck.filter(c => /\.x\.com$/i.test(c.domain || ''));
    const needX  = tw.filter(c => !x.some(d => d.name === c.name));
    const needTw = x.filter(c => !tw.some(d => d.name === c.name));
    if (needX.length)  await context.addCookies(needX.map(c => ({ ...c, domain: '.x.com' })));
    if (needTw.length) await context.addCookies(needTw.map(c => ({ ...c, domain: '.twitter.com' })));
  } catch (e) {
    console.log('[INFO] Cookie bridge skipped:', e.message);
  }

  // Save artifacts
  await context.storageState({ path: STATE_PATH });
  const cookies = await context.cookies();
  fs.writeFileSync(COOKIES_PATH, JSON.stringify(cookies, null, 2));

  const auth = cookies.find(c => c.name === 'auth_token');
  console.log('[✅] Logged in — saved state.json + cookies.json');
  console.log('[auth_token length]', auth?.value?.length || 0);

  await context.close();
})();
