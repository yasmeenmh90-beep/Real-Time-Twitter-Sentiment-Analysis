# dashboard/views.py

from datetime import timedelta, date, datetime
import re, collections, numpy as np
import redis
from collections import Counter
import math
from datetime import datetime as _dt
from django.core.paginator import Paginator
from django.http import JsonResponse
from django.shortcuts import render
from django.utils import timezone

from pymongo import MongoClient, DESCENDING
from bson.regex import Regex
from django.utils import timezone as djtz
from dateutil import parser as _parser
from django.views.decorators.cache import cache_page

# Atlas collections yahan se aayengi
from dashboard.mongo_utils import coll_latest, coll_history

MONGO_URI = os.getenv("MONGO_URI")

MONGO_DB="twitter_rt"
MONGO_COLL="scored_tweets"

_client = MongoClient(f"{MONGO_URI_BASE}/{MONGO_DB}?retryWrites=true&w=majority")
_coll   = _client[MONGO_DB][MONGO_COLL]

# label maps
LBL2INT = {"positive": 1, "neutral": 2, "negative": 0}
INT2LBL = {1: "positive", 2: "neutral", 0: "negative"}

HASHTAG_RX = re.compile(r"#(\w+)")
WORD_RX    = re.compile(r"[A-Za-z']{3,}")

# ===== Sentiment matching helper =====
SENT_TXT2NUM = {"positive": 1, "neutral": 2, "negative": 0}
SENT_ALIASES = {"pos": "positive", "neu": "neutral", "neg": "negative",
                "1": "positive", "2": "neutral", "0": "negative"}
# ─────────────────────────────── live page ─────────────────────────────
def home(request):
    cards = [
        ("POSITIVE", "pos", "success", "bi-emoji-smile"),
        ("NEUTRAL",  "neu", "secondary", "bi-dash-circle"),
        ("NEGATIVE", "neg", "danger", "bi-emoji-angry"),
        ("TWEETS / MIN", "total", "primary", "bi-graph-up"),
    ]
    # sentiment ints → counts from latest collection
    agg = list(coll_latest().aggregate([
        {"$group": {"_id": "$sentiment", "c": {"$sum": 1}}}
    ]))
    counts = {"pos": 0, "neu": 0, "neg": 0}
    for r in agg:
        s = r["_id"]
        if s == 1:
            counts["pos"] = r["c"]
        elif s == 2:
            counts["neu"] = r["c"]
        elif s == 0:
            counts["neg"] = r["c"]

    return render(request, "dashboard_frontend/home.html", {
        "cards": cards,
        "sentiment_counts": counts,
    })

# --- helpers (keep once) ---

def _label_norm(s):
    if isinstance(s, int):
        s = INT2LBL.get(s, "neutral")
    s = (str(s) or "").lower()
    if s.startswith("pos"):
        return "positive"
    if s.startswith("neg"):
        return "negative"
    if s.startswith("neu"):
        return "neutral"
    return "neutral"

_LABEL_CODE = {"positive": 2, "negative": 0, "neutral": 1}
def _label_code(x):
    return _LABEL_CODE[_label_norm(x)]
# -----------------------------------------------------------------------
def _pretty(lbl):
    return {"positive": "😊 Positive",
            "neutral":  "😐 Neutral",
            "negative": "😡 Negative"}[lbl]

def _parse_ts(ts):
    """Accept datetime or str (ISO/ts); return datetime|None."""
    if isinstance(ts, datetime):
        return ts
    if not ts:
        return None
    s = str(ts).replace("Z", "")
    try:
        # try fromisoformat first
        return datetime.fromisoformat(s)
    except Exception:
        try:
            # fallback to dateutil if available
            from dateutil import parser
            return parser.parse(s)
        except Exception:
            return None

# ---------------- API ----------------
def api_metrics(request):
    col = coll_latest()

    # ---------- counts by sentiment ----------
    pipeline = [{"$group": {"_id": "$sentiment", "count": {"$sum": 1}}}]
    result = list(col.aggregate(pipeline))

    pos = neu = neg = 0
    INT2LBL = {2: "positive", 1: "neutral", 0: "negative"}

    for r in result:
        sid = r.get("_id")
        label = INT2LBL.get(sid, "neutral") if isinstance(sid, int) else (str(sid) or "").lower()
        if label.startswith("pos"): pos = r["count"]
        elif label.startswith("neu"): neu = r["count"]
        elif label.startswith("neg"): neg = r["count"]

    # UI-compat: short + full keys
    counts = {
        "pos": pos, "neu": neu, "neg": neg,
        "positive": pos, "neutral": neu, "negative": neg,
        "total": pos + neu + neg,
    }
    # ---------- latest tweets ----------
    projection = {
        "_id": 0,
        "tweet_id": 1, "ids": 1, "id": 1,
        "text": 1,
        "clean_text": 1, "Clean_text": 1, "processed_text": 1,
        "source": 1, "source_device": 1, "app_source": 1,
        "origin": 1, "backfill": 1,
        "created_at": 1, "date_ts": 1, "timestamp": 1,
        "sentiment": 1, "sentiment_label": 1,
        "lang": 1,
    }

    latest_cursor = col.find({}, projection).sort(
        [("created_at", -1), ("date_ts", -1), ("timestamp", -1)]
    ).limit(2000)

    now = datetime.utcnow()
    latest = []
    for d in latest_cursor:
        tid = d.get("tweet_id") or d.get("ids") or d.get("id") or ""
        text = d.get("text") or ""
        ctext = d.get("clean_text") or d.get("Clean_text") or d.get("processed_text") or ""

        lbl = _label_norm(d.get("sentiment"))
        pretty = _pretty(lbl)

        source = d.get("source") or d.get("source_device") or d.get("app_source") or "scorer"

        raw_ts = d.get("created_at") or d.get("date_ts") or d.get("timestamp")
        ts_dt = _parse_ts(raw_ts)
        iso_ts = ts_dt.isoformat() if ts_dt else str(raw_ts or "")

        if d.get("origin"):
            origin = d["origin"]
        elif d.get("backfill") is True:
            origin = "backfill"
        elif ts_dt and (now - ts_dt) <= timedelta(minutes=10):
            origin = "live"
        elif ts_dt and ts_dt.date() == now.date():
            origin = "today"
        else:
            origin = "backfill"
            # include BOTH 'sentiment' + 'sentiment_label' + aliases
            lbl = _label_norm(d.get("sentiment"))
            code = _label_code(lbl)

            latest.append({
                "tweet_id": tid,
                "id": tid,
                "text": text,
                "clean_text": ctext,
                "source": source,
                "origin": origin,
                "sentiment": lbl,  # <-- string (positive/negative/neutral)
                "sentiment_label": code,  # <-- numeric (1/0/2)  ✅
                "timestamp": iso_ts,
                "time": iso_ts,
                "created_at": iso_ts,
                "lang": d.get("lang") or "",
            })
            # ---------- hashtags & words ----------
        HASHTAG_RX = re.compile(r"#(\w+)")
        WORD_RX = re.compile(r"[A-Za-z']{3,}")

        hashtag_counter = Counter()
        word_counter = Counter()

        for t in col.find({}, {"text": 1}).sort(
                [("created_at", -1), ("date_ts", -1)]
        ).limit(800):
            tx = t.get("text") or ""
            hashtag_counter.update(h.lower() for h in HASHTAG_RX.findall(tx))
            word_counter.update(w.lower() for w in WORD_RX.findall(tx))

        hashtags = [{"tag": f"#{t}", "count": c} for t, c in hashtag_counter.most_common(10)]
        words = [{"word": w, "weight": c} for w, c in word_counter.most_common(100)]

        # ---------- hourly & DOW ----------
        hourly_bucket, dow_bucket = {}, {}
        for t in col.find({}, {"created_at": 1, "date_ts": 1, "timestamp": 1}).limit(2000):
            ts = t.get("created_at") or t.get("date_ts") or t.get("timestamp")
            dt = _parse_ts(ts)
            if dt:
                hourly_bucket[dt.hour] = hourly_bucket.get(dt.hour, 0) + 1
                dow_bucket[dt.weekday()] = dow_bucket.get(dt.weekday(), 0) + 1

        hourly = [{"hour": h, "count": hourly_bucket.get(h, 0)} for h in range(24)]
        dow = [{"dow": d, "count": dow_bucket.get(d, 0)} for d in range(7)]

        # ---------- response ----------
        return JsonResponse(
            {
                "counts": counts,
                "tweets": latest,
                "hashtags": hashtags,
                "words": words,
                "hourly": hourly,
                "dow": dow,
            },
            json_dumps_params={"default": str},
        )
def api_latest(request):
    n = int(request.GET.get("n", 10))
    latest = list(coll_latest().find(
        {}, {"_id": 0, "text": 1, "sentiment": 1, "created_at": 1}
    ).sort("created_at", -1).limit(n))
    return JsonResponse(latest, safe=False, json_dumps_params={"default": str})

def export_live_csv(request):
    col = coll_latest()
    now = djtz.now()

    minutes   = int(request.GET.get("minutes", 10))   # last X minutes
    origin    = request.GET.get("origin")             # live|backfill|all
    sentiment = request.GET.get("sentiment")          # positive|neutral|negative|all
    limit     = int(request.GET.get("limit", 2500))   # safety cap

    match = {"created_at": {"$gte": now - timedelta(minutes=minutes)}}
    if origin in {"live", "backfill"}:
        match["origin"] = origin
    if sentiment and sentiment.lower() in {"positive","neutral","negative"}:
        s = {"positive":1, "neutral":2, "negative":0}[sentiment.lower()]
        match["sentiment"] = s

    cur = col.find(match).sort([("created_at",-1)]).limit(limit)

    # CSV in-memory
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["tweet_id", "user", "text", "clean_text", "sentiment", "created_at", "origin", "favorite_count",
                "retweet_count"])
    for d in cur:
        w.writerow([
            d.get("tweet_id") or d.get("_id"),
            d.get("user") or d.get("screen_name") or d.get("author") or "",
            (d.get("text") or "").replace("\n", " "),
            (d.get("clean_text") or d.get("processed_text") or d.get("text") or "").replace("\n", " "),
            INT2LBL.get(d.get("sentiment"), ""),
            str(d.get("created_at") or ""),
            d.get("origin", ""),
            d.get("favorite_count") or 0,
            d.get("retweet_count") or 0,
        ])

    resp = HttpResponse(buf.getvalue(), content_type="text/csv; charset=utf-8")
    resp["Content-Disposition"] = 'attachment; filename="live_tweets.csv"'
    return resp

def coll_latest():
    # agar future me tum rotation/alias use karo to yahan switch karna
    return _coll
# --- helpers ---
def build_sentiment_clause(q: str | None):
    """
    Accept 'positive'|'neutral'|'negative'|'all'/None.
    Returns a Mongo $or clause that matches both string/int fields.
    """
    s = (q or "").strip().lower()
    if s in ("positive", "neutral", "negative"):
        code_map = {"positive": 1, "neutral": 2, "negative": 0}
        code = code_map[s]
        return {
            "$or": [
                {"sentiment": s},              # string
                {"sentiment": code},           # int
                {"sentiment_label": s},        # string (if exists)
                {"sentiment_label": code},     # int (if exists)
                {"sentiment_label": {"$regex": s[:3], "$options": "i"}},  # safety
            ]
        }
    return None

def _parse_sentiment(q):
    s = (q or "").lower()
    if s.startswith("pos"): return "positive"
    if s.startswith("neg"): return "negative"
    if s.startswith("neu"): return "neutral"
    return None

def _parse_ymd(s: str | None):
    """'YYYY-MM-DD' -> datetime (00:00), else None"""
    if not s:
        return None
    try:
        return _dt.strptime(s.strip(), "%Y-%m-%d")
    except Exception:
        return None

# ───────── Timeline helpers ─────────
def _coalesce_user(d):
    return d.get("user") or d.get("screen_name") or d.get("author") or ""

def _time_match(*, minutes=None, hours=None, start=None, end=None):
    """Mongo match for created_at window."""
    now = djtz.now()
    if start or end:
        start_dt = _parse_ymd(start) if start else None
        end_dt   = _parse_ymd(end)   if end   else None
        if start_dt and end_dt:
            return {"created_at": {"$gte": start_dt, "$lt": end_dt + timedelta(days=1)}}
        if start_dt:
            return {"created_at": {"$gte": start_dt}}
        if end_dt:
            return {"created_at": {"$lt": end_dt + timedelta(days=1)}}
    if minutes:
        return {"created_at": {"$gte": now - timedelta(minutes=int(minutes))}}
    if hours:
        return {"created_at": {"$gte": now - timedelta(hours=int(hours))}}
    # default = today
    start_today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return {"created_at": {"$gte": start_today}}

# positive/neutral/negative -> both text/numeric
LBL2INT = {"positive": 2, "neutral": 1, "negative": 0}
def _sentiment_clause(s):
    s = (s or "").strip().lower()
    if not s or s == "all":
        return None
    ors = [{"sentiment": s}]
    if s in LBL2INT:
        ors.append({"sentiment_label": LBL2INT[s]})
    return {"$or": ors}

def _project_row(d):
    # sentiment ko words me normalize
    raw = d.get("sentiment")
    if isinstance(raw, int):
        lbl = INT2LBL.get(raw, "neutral")
        num_label = raw
    else:
        lbl = (str(raw or "")).lower()
        if lbl.startswith("pos"): lbl = "positive"
        elif lbl.startswith("neg"): lbl = "negative"
        elif lbl.startswith("neu"): lbl = "neutral"
        else: lbl = "neutral"
        num_label = LBL2INT.get(lbl, 2)

    return {
        "tweet_id": d.get("tweet_id") or d.get("ids") or d.get("id"),
        "text": d.get("text") or "",
        "clean_text": d.get("clean_text") or d.get("processed_text") or "",
        "sentiment": lbl,  # words
        "sentiment_label": num_label,  # 0/1/2
        "source": d.get("source") or d.get("source_device") or d.get("app_source") or "scorer",
        "origin": d.get("origin") or "live",
        "timestamp": d.get("created_at") or d.get("date_ts") or d.get("timestamp"),
    }
# ───────── Timeline page ─────────
def timeline(request):
    """Single, clean Timeline page (cards)."""
    return render(request, "dashboard_frontend/timeline.html", {})

# ───────── API: 1) Spike Radar ─────────
def api_tl_bursts(request):
    """
    ?mins=60  (default 60)
    optional: &sentiment=positive|neutral|negative|all
    """
    col   = coll_latest()
    mins  = int(request.GET.get("mins", 60))
    sent  = request.GET.get("sentiment", "all")

    match = _time_match(minutes=mins)
    s = _sentiment_clause(sent)
    if s: match = {"$and": [match, s]}

    # pull last N minutes tweets' created_at only (fast)
    cur = col.find(match, {"created_at": 1}).sort("created_at", -1).limit(20000)

    # count per minute in python to avoid server-side $date ops
    now = djtz.now().replace(second=0, microsecond=0)
    buckets = {now - timedelta(minutes=i): 0 for i in range(mins)}
    for d in cur:
        ts = d.get("created_at")
        dt = ts if isinstance(ts, _dt) else _parse_ts(ts)
        if not dt:
            continue
        key = dt.replace(second=0, microsecond=0)
        if key in buckets:
            buckets[key] += 1

    # return in ascending time
    data = [{"t": k.isoformat(), "count": buckets[k]} for k in sorted(buckets.keys())]
    return JsonResponse({"points": data})

# ───────── API: 2) Momentum (top engaging) ─────────
def api_tl_momentum(request):
    """
    ?hours=6&limit=50&sentiment=all
    score = favorite_count + retweet_count
    """
    col    = coll_latest()
    hours  = int(request.GET.get("hours", 6))
    limit  = int(request.GET.get("limit", 50))
    sent   = request.GET.get("sentiment", "all")

    match  = _time_match(hours=hours)
    s = _sentiment_clause(sent)
    if s: match = {"$and": [match, s]}

    pipeline = [
        {"$match": match},
        {"$addFields": {
            "score": {"$add": [
                {"$ifNull": ["$favorite_count", 0]},
                {"$ifNull": ["$retweet_count", 0]}
            ]}
        }},
        {"$sort": {"score": -1, "created_at": -1}},
        {"$limit": limit},
        {"$project": {
            "_id": 0,
            "tweet_id": {"$ifNull": ["$tweet_id", "$_id"]},
            "user": {"$ifNull": ["$user", {"$ifNull": ["$screen_name", {"$ifNull": ["$author", ""]}]}]},
            "text": 1,
            "clean_text": {"$ifNull": ["$clean_text", {"$ifNull": ["$processed_text", ""]}]},
            "created_at": 1,
            "sentiment": 1,
            "sentiment_label": 1,
            "favorite_count": {"$ifNull": ["$favorite_count", 0]},
            "retweet_count": {"$ifNull": ["$retweet_count", 0]},
            "score": 1
        }},
    ]
    rows = list(col.aggregate(pipeline))
    return JsonResponse({"rows": rows})


# ───────── API: 3) Leaders ─────────
def api_tl_leaders(request):
    """
    Posters bringing engagement (original posts).
    ?hours=6&limit=20
    """
    col    = coll_latest()
    hours  = int(request.GET.get("hours", 6))
    limit  = int(request.GET.get("limit", 20))

    match  = _time_match(hours=hours)
    # try to exclude “RT ...” originals
    match = {"$and": [match, {"text": {"$not": Regex(r"^RT\s", "i")}}]}

    pipeline = [
        {"$match": match},
        {"$addFields": {
            "u": {"$ifNull": ["$user", {"$ifNull": ["$screen_name", {"$ifNull": ["$author", ""]}]}]},
            "eng": {"$add": [
                {"$ifNull": ["$favorite_count", 0]},
                {"$ifNull": ["$retweet_count", 0]}
            ]}
        }},
        {"$group": {"_id": "$u", "posts": {"$sum": 1}, "engagement": {"$sum": "$eng"}}},
        {"$sort": {"engagement": -1, "posts": -1}},
        {"$limit": limit},
        {"$project": {"user": "$_id", "_id": 0, "posts": 1, "engagement": 1}},
    ]
    rows = list(col.aggregate(pipeline))
    return JsonResponse({"rows": rows})


# ───────── API: 4) Amplifiers ─────────
def api_tl_amplifiers(request):
    """
    Heavy retweeters/quoters.
    ?hours=6&limit=20
    """
    col = coll_latest()
    hours = int(request.GET.get("hours", 6))
    limit = int(request.GET.get("limit", 20))

    match = _time_match(hours=hours)
    # very simple RT detector; adjust if you have dedicated flags
    match = {"$and": [match, {"text": {"$regex": r"^RT\s", "$options": "i"}}]}

    pipeline = [
        {"$match": match},
        {"$addFields": {
            "u": {"$ifNull": ["$user", {"$ifNull": ["$screen_name", {"$ifNull": ["$author", ""]}]}]},
        }},
        {"$group": {"_id": "$u", "retweets": {"$sum": 1}}},
        {"$sort": {"retweets": -1}},
        {"$limit": limit},
        {"$project": {"user": "$_id", "_id": 0, "retweets": 1}},
    ]
    rows = list(col.aggregate(pipeline))
    return JsonResponse({"rows": rows})

# ───────── API: 5) Emoji Meter (optional) ─────────
EMOJI_RX = re.compile(r"[\U0001F300-\U0001FAFF\U00002700-\U000027BF]")
def api_tl_emojis(request):
    """
    ?hours=24&limit=2000 (docs to scan)
    returns emoji + total + pos/neu/neg
    """
    col   = coll_latest()
    hours = int(request.GET.get("hours", 24))
    cap   = int(request.GET.get("limit", 2000))

    match = _time_match(hours=hours)
    cur = col.find(match, {"text": 1, "sentiment": 1}).sort("created_at", -1).limit(cap)

    counts = {}
    for d in cur:
        text = d.get("text") or ""
        emos = EMOJI_RX.findall(text)
        if not emos:
            continue
        raw = d.get("sentiment")
        lbl = INT2LBL.get(raw, "neutral") if isinstance(raw, int) else _label_norm(raw)
        for e in emos:
            c = counts.setdefault(e, {"emoji": e, "count": 0, "pos": 0, "neu": 0, "neg": 0})
            c["count"] += 1
            if lbl == "positive": c["pos"] += 1
            elif lbl == "negative":
                c["neg"] += 1
            else:
                c["neu"] += 1

        rows = sorted(counts.values(), key=lambda x: x["count"], reverse=True)[:50]
        return JsonResponse({"rows": rows})

    def _mk_match(date_sel: str, sentiment_unused=None, *, start_str: str | None = None, end_str: str | None = None):
        now = djtz.now()
        date_sel = (date_sel or "today").lower()

        # explicit start/end override if both present
        if start_str or end_str:
            start_dt = _parse_ymd(start_str) if start_str else None
            end_dt = _parse_ymd(end_str) if end_str else None
            if start_dt and end_dt:
                return {"created_at": {"$gte": start_dt, "$lt": end_dt + timedelta(days=1)}}
            if start_dt and not end_dt:
                return {"created_at": {"$gte": start_dt}}
            if end_dt and not start_dt:
                return {"created_at": {"$lt": end_dt + timedelta(days=1)}}

        # else fall back to quick selectors
        if date_sel == "yesterday":
            start = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            end = start + timedelta(days=1)
            return {"created_at": {"$gte": start, "$lt": end}}
        if date_sel == "all":
            return {}
            # today
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        return {"created_at": {"$gte": start}}


def api_history(request):
    col = coll_latest()  # ya coll_history() agar wahin se dikhana ho

    # params
    date_sel = (request.GET.get("date") or "today").lower()
    sentiment = request.GET.get("sentiment")
    start_str = request.GET.get("start")
    end_str = request.GET.get("end")

    try:
        page = max(1, int(request.GET.get("page", 1)))
    except Exception:
        page = 1
    try:
        page_size = int(request.GET.get("page_size", 5000))
    except Exception:
        page_size = 5000
    page_size = max(50, min(page_size, 5000))
    skip = (page - 1) * page_size

    mode = (request.GET.get("sort") or "latest").lower()

    # build query (date+sentiment)
    match = _mk_match(date_sel, None, start_str=start_str, end_str=end_str)
    s_clause = build_sentiment_clause(sentiment)
    query = match if not s_clause else {"$and": [match, s_clause]}

    total = col.count_documents(query)
    total_pages = max(1, math.ceil(total / page_size))

    if mode == "trending":
        pipeline = [
            {"$match": query},
            {"$addFields": {
                "score": {"$add": [
                    {"$ifNull": ["$favorite_count", 0]},
                    {"$ifNull": ["$retweet_count", 0]},
                ]}
            }},
            {"$sort": {"score": -1, "created_at": -1}},
            {"$skip": skip},
            {"$limit": page_size},
        ]
        docs = col.aggregate(pipeline)
        rows = [_project_row(d) for d in docs]
    else:
        cursor = (col.find(query)
                  .sort([("created_at", -1), ("date_ts", -1)])
                  .skip(skip).limit(page_size))
        rows = [_project_row(d) for d in cursor]

    showing_start = 0 if total == 0 else (skip + 1)
    showing_end = min(skip + len(rows), total)

    return JsonResponse({
        "rows": rows,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "showing_start": showing_start,
        "showing_end": showing_end,
    })
# ───────────────────────────── history page ────────────────────────────
from django.core.paginator import Paginator
from django.utils import timezone as djtz


def history(request):
    sort_mode = (request.GET.get("sort") or "latest").lower()
    date_sel  = (request.GET.get("date") or "today").lower()
    s_param   = (request.GET.get("sentiment") or "all").lower()

    # paging
    try:    page_number = max(1, int(request.GET.get("page", 1)))
    except: page_number = 1
    try:    page_size = int(request.GET.get("page_size", 5000))
    except: page_size = 5000
    page_size = max(50, min(page_size, 5000))
    skip = (page_number - 1) * page_size

    col   = coll_history()
    start_str = request.GET.get("start")
    end_str   = request.GET.get("end")
    match = _mk_match(date_sel, None, start_str=start_str, end_str=end_str)

    s_clause = build_sentiment_clause(s_param)
    query = match if not s_clause else {"$and": [match, s_clause]}

    total = col.count_documents(query)
    total_pages = max(1, math.ceil(total / page_size))

    if sort_mode == "trending":
        pipeline = [
            {"$match": query},
            {"$addFields": {
                "engagement_score": {"$add": [
                    {"$ifNull": ["$favorite_count", 0]},
                    {"$ifNull": ["$retweet_count", 0]}
                ]}
            }},
            {"$sort": {"engagement_score": -1, "created_at": -1}},
            {"$skip": skip},
            {"$limit": page_size},
        ]
        docs = list(col.aggregate(pipeline))
    else:
        docs = list(
            col.find(query)
            .sort([("created_at", -1), ("date_ts", -1)])
            .skip(skip)
            .limit(page_size)
        )

        formatted = []
        for tw in docs:
            ts = tw.get("created_at")
            if isinstance(ts, _dt):
                created_at_str = ts.strftime("%Y-%m-%d %H:%M:%S")
            else:
                try:
                    created_at_str = _parser.parse(str(ts)).strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    created_at_str = ""

            # normalize both text + numeric label
            raw_sent = tw.get("sentiment")
            if isinstance(raw_sent, int):
                lbl_txt = INT2LBL.get(raw_sent, "neutral")
                lbl_num = raw_sent
            else:
                lbl_txt = (str(raw_sent or "")).lower()
                if lbl_txt.startswith("pos"):
                    lbl_txt = "positive"
                elif lbl_txt.startswith("neg"):
                    lbl_txt = "negative"
                elif lbl_txt.startswith("neu"):
                    lbl_txt = "neutral"
                else:
                    lbl_txt = "neutral"
                lbl_num = LBL2INT.get(lbl_txt, 2)

                formatted.append({
                    "ids": tw.get("tweet_id", "") or str(tw.get("_id", "")),
                    "user": tw.get("user", "") or tw.get("screen_name", "") or tw.get("author", ""),
                    "text": tw.get("text", ""),
                    "processed_text": tw.get("clean_text", "") or tw.get("processed_text", "") or tw.get("text", ""),
                    "vader_score": tw.get("vader_score", None),

                    "sentiment": lbl_txt,  # text
                    "sentiment_label": lbl_num,  # 0/1/2 ✅
                    "created_at": created_at_str,

                    "text_length": tw.get("text_length", ""),
                    "word_count": tw.get("word_count", ""),
                    "char_density": tw.get("char_density", ""),
                    "capital_word_count": tw.get("capital_word_count", ""),
                    "negation_count": tw.get("negation_count", ""),
                    "emoji_count": tw.get("emoji_count", ""),
                    "is_question": tw.get("is_question", False),
                    "has_mentions": tw.get("has_mentions", False),
                    "has_hashtags": tw.get("has_hashtags", False),
                    "has_links": tw.get("hasLink", False),
                    "hashtags": re.findall(r"#\w+", tw.get("text", "") or ""),
                    "engagement_score": (tw.get("favorite_count") or 0) + (tw.get("retweet_count") or 0),
                })

                paginator = Paginator(range(total), page_size)
                page_obj = paginator.get_page(page_number)

                context = {
                    "mode": "trending" if sort_mode == "trending" else "new",
                    "grain": "day",
                    "tweets": formatted,
                    "page_obj": page_obj,
                    "page_size": page_size,
                    "total": total,
                    "showing_start": 0 if total == 0 else (skip + 1),
                    "showing_end": min(skip + len(formatted), total),
                    "sentiment_filter": "" if s_param == "all" else s_param,
                }
                return render(request, "dashboard_frontend/history.html", context)

# ─────────────────────── stacked-bar JSON for history ──────────────────
def api_sentiment_bars(request):
    grain = request.GET.get("grain", "day")
    mode  = request.GET.get("mode",  "day")
    now   = datetime.utcnow()
    start = now - timedelta(days=7) if mode == "week" else datetime(now.year, now.month, now.day)

    res = list(coll_history().aggregate([
        {"$match": {"created_at": {"$gte": start}}},
        {"$group": {
            "_id": {
                "bucket": {
                    "$dateToString": {
                        "format": "%Y-%m-%d" if grain == "day" else "%Y-W%V",
                        "date": "$created_at"
                    }
                },
                "s": "$sentiment"     # int
            },
            "c": {"$sum": 1}
        }}
    ]))

    grouped = {}
    for r in res:
        b = r["_id"]["bucket"]
        s = r["_id"]["s"]
        key = {1: "pos", 2: "neu", 0: "neg"}.get(s)
        if not key:
            continue
        grouped.setdefault(b, {"pos": 0, "neu": 0, "neg": 0})
        grouped[b][key] = r["c"]

    bars = [{"label": b, **vals, "total": vals["pos"] + vals["neu"] + vals["neg"]}
            for b, vals in sorted(grouped.items())]
    return JsonResponse(bars, safe=False)

# ─────────────────────── Other APIs (Mongo-based) ──────────────────────
def api_language_pie(request):
    """
    Return: [{"lang":"en","count":123}, ...] for the last X hours (default 24).
    Counts both `lang` and `language` fields, ignores empties, fast aggregation.
    Query params:
      - hours: int (default 24)
      - limit: int (default 20)
    """
    col = coll_latest()

    # read query params (with safe defaults)
    try:
        hours = int(request.GET.get("hours", 24))
    except Exception:
        hours = 24
    try:
        limit = int(request.GET.get("limit", 20))
    except Exception:
        limit = 20

    since = djtz.now() - timedelta(hours=hours)

    pipeline = [
        # Time window: accept string or Date created_at
        {"$match": {"$expr": {"$gte": [{"$toDate": "$created_at"}, since]}}},

        # Pick lang or language, trim + lowercase
        {"$project": {
            "lg": {
                "$toLower": {
                    "$trim": {"input": {"$ifNull": ["$lang", "$language"]}}
                }
            }
        }},

        # Ignore missing/empty
        {"$match": {"lg": {"$ne": None, "$ne": ""}}},

        # Count, sort, limit
        {"$group": {"_id": "$lg", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": limit},

        # Shape output
        {"$project": {"_id": 0, "lang": "$_id", "count": 1}}
    ]

    try:
        rows = list(col.aggregate(pipeline))
        return JsonResponse(rows, safe=False)
    except Exception:
        # If aggregation ever fails, return empty (front-end still works live)
        return JsonResponse([], safe=False)


def api_top_users(request):
    col = coll_latest()
    since = djtz.now() - timedelta(minutes=10)
    pipeline = [
        {"$match": {"created_at": {"$gte": since}}},
        {"$project": {"u": {"$ifNull": ["$user", {"$ifNull": ["$screen_name", ""]}]}}},
        {"$group": {"_id": "$u", "n": {"$sum": 1}}},
        {"$sort": {"n": -1}}, {"$limit": 10}
    ]
    rows = list(col.aggregate(pipeline))
    return JsonResponse({"labels": [r["_id"] or "?" for r in rows], "values": [r["n"] for r in rows]})


def api_hashtag_cloud(request):
    all_tags = []
    for doc in coll_latest().find({}, {"text": 1}).limit(1000):
        tags = re.findall(r"#\w+", doc.get("text", "") or "")
        all_tags.extend([t.lower() for t in tags])
    top = collections.Counter(all_tags).most_common(20)
    return JsonResponse([{"tag": t, "count": c} for t, c in top], safe=False)


def api_tweets_by_view(request):
    mode = request.GET.get("mode", "day")
    now = datetime.utcnow()
    if mode == "hour":
        start = now - timedelta(hours=1)
    elif mode == "week":
        start = now - timedelta(days=7)
    else:
        start = datetime(now.year, now.month, now.day)

    q = {"created_at": {"$gte": start}}
    tweets = list(coll_history().find(q).sort("created_at", -1).limit(50))
    results = []
    for t in tweets:
        results.append({
            "user": t.get("user", ""),
            "processed_text": t.get("clean_text", "") or t.get("processed_text", "") or t.get("text", ""),
            "sentiment": INT2LBL.get(t.get("sentiment"), "neutral"),
            "created_at": str(t.get("created_at", "")),
        })
    return JsonResponse(results, safe=False)

def api_custom_hashtag(request):
    tag = request.GET.get("tag")
    if not tag:
        return JsonResponse({"tweets": []})
    regex = Regex(tag, "i")
    cursor = coll_latest().find({"text": regex}, {"text": 1}).limit(20)
    texts = [doc.get("text", "") for doc in cursor]
    return JsonResponse({"tweets": texts})

def api_hourly_trend(request):
    """24h hourly trend using Mongo (not Django ORM)."""
    now = datetime.utcnow()
    start = now - timedelta(hours=24)
    q = {"created_at": {"$gte": start}}
    rows = list(coll_history().find(q, {"created_at": 1, "sentiment": 1}))
    buckets = {}
    for r in rows:
        ts = r.get("created_at")
        if not isinstance(ts, datetime):
            continue
        hour = ts.replace(minute=0, second=0, microsecond=0)
        key = hour.isoformat()
        buckets.setdefault(key, {"pos": 0, "neu": 0, "neg": 0})
        lbl = {2: "pos", 1: "neu", 0: "neg"}.get(r.get("sentiment"), "neu")
        buckets[key][lbl] += 1

        result = []
        for k in sorted(buckets.keys()):
            result.append({
                "hour": datetime.fromisoformat(k).strftime("%H:%M"),
                "pos": buckets[k]["pos"],
                "neu": buckets[k]["neu"],
                "neg": buckets[k]["neg"],
            })
        return JsonResponse(result, safe=False)

    @cache_page(60)  # 60s cache = fast
    def api_forecast(request):
        col = coll_latest()
        now = djtz.now()
        since = now - timedelta(hours=1)

        pipeline = [
            # Keep rows whose created_at (string or date) >= since
            {"$match": {
                "$expr": {"$gte": [{"$toDate": "$created_at"}, since]}
            }},

            # Bucket per-minute and normalize sentiment to code 1/2/0
            {"$project": {
                "bucket": {"$dateTrunc": {"date": {"$toDate": "$created_at"}, "unit": "minute"}},
                "s": {
                    "$let": {"vars": {"s": "$sentiment"}, "in": {
                        "$switch": {
                            "branches": [
                                {"case": {"$or": [{"$eq": ["$$s", 1]},
                                                  {"$regexMatch": {"input": {"$toString": "$$s"}, "regex": r"^pos",
                                                                   "options": "i"}}]},
                                 "then": 1},
                                {"case": {"$or": [{"$eq": ["$$s", 2]},
                                                  {"$regexMatch": {"input": {"$toString": "$$s"}, "regex": r"^neu",
                                                                   "options": "i"}}]},
                                 "then": 2},
                                {"case": {"$or": [{"$eq": ["$$s", 0]},
                                                  {"$regexMatch": {"input": {"$toString": "$$s"}, "regex": r"^neg",
                                                                   "options": "i"}}]},
                                 "then": 0}
                            ],
                            "default": -1
                        }
                    }}
                }
            }},
            # Per-minute counts by sentiment
            {"$group": {"_id": {"b": "$bucket", "s": "$s"}, "n": {"$sum": 1}}},

            # Flatten to { _id: minute, pos, neu, neg }
            {"$group": {
                "_id": "$_id.b",
                "pos": {"$sum": {"$cond": [{"$eq": ["$_id.s", 1]}, "$n", 0]}},
                "neu": {"$sum": {"$cond": [{"$eq": ["$_id.s", 2]}, "$n", 0]}},
                "neg": {"$sum": {"$cond": [{"$eq": ["$_id.s", 0]}, "$n", 0]}},
            }},
            {"$sort": {"_id": 1}}
        ]
        series = list(col.aggregate(pipeline))

        # Exponential moving average → tweets per minute
        def ema_rate(key, alpha=0.2):
            rate = 0.0
            for row in series:
                rate = alpha * float(row.get(key, 0) or 0) + (1 - alpha) * rate
            return rate

        r_pos = ema_rate("pos")
        r_neu = ema_rate("neu")
        r_neg = ema_rate("neg")

        # Forecast next 6 hours as (rate/min * 60) per hour
        labels, pos, neu, neg = [], [], [], []
        for h in range(1, 7):
            labels.append((now + timedelta(hours=h)).strftime("%H:00"))
            pos.append(int(round(r_pos * 60)))
            neu.append(int(round(r_neu * 60)))
            neg.append(int(round(r_neg * 60)))

        return JsonResponse({"labels": labels, "pos": pos, "neu": neu, "neg": neg})

    redis_conn = redis.Redis.from_url("redis://localhost:6379")

    def redis_status(request):
        buffer_len = redis_conn.llen("tweets:buffer")
        replayed = redis_conn.lrange("replayed:ids", 0, 19)
        replayed = [rid.decode() for rid in replayed]

        return JsonResponse({
            "buffer_size": buffer_len,
            "recent_replays": replayed
        })

from django.http import JsonResponse
from django.views import View

class ReplayBufferedTweets(View):
    def get(self, request):
        r = redis.Redis.from_url("redis://localhost:6379")
        buffered = r.lrange("tweets:buffer", 0, -1)
        return JsonResponse({"buffer_count": len(buffered)})

def api_language_distribution(request):
    col = coll_latest()
    pipeline = [
        {"$match": {"language": {"$exists": True, "$ne": None}}},
        {"$group": {"_id": "$language", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10},
    ]
    data = list(col.aggregate(pipeline))
    return JsonResponse([{"language": d["_id"], "count": d["count"]} for d in data], safe=False)
def api_top_hashtags(request):
    col = coll_latest()

    # how many docs to scan (recent first)
    try:
        sample = int(request.GET.get("sample", 2000))
    except Exception:
        sample = 2000
    sample = max(200, min(sample, 5000))

    try:
        limit = int(request.GET.get("limit", 20))
    except Exception:
        limit = 20
    limit = max(5, min(limit, 50))

    cur = col.find({}, {"text": 1, "clean_text": 1, "created_at": 1}) \
        .sort([("created_at", -1)]) \
        .limit(sample)

    rx = re.compile(r"#\w+")
    counter = Counter()
    for d in cur:
        text = (d.get("text") or "") + " " + (d.get("clean_text") or "")
        for tag in rx.findall(text):
            counter[tag.lower()] += 1

    data = [{"hashtag": tag, "count": cnt} for tag, cnt in counter.most_common(limit)]
    return JsonResponse(data, safe=False)


def kpis(request):
    """
    Return sentiment counts + rate_per_min for cards.
    Query params:
      - date: today | yesterday | last1h | all (default: today)
      - origin: live | backfill (optional)
    """
    col = coll_latest()
    now = datetime.now(timezone.utc)

    date_sel = (request.GET.get("date") or "today").strip().lower()
    origin = (request.GET.get("origin") or "").strip().lower()

    match = {}

    if date_sel == "today":
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
        match["created_at"] = {"$gte": start, "$lt": end}
    elif date_sel == "yesterday":
        end = now.replace(hour=0, minute=0, second=0, microsecond=0)
        start = end - timedelta(days=1)
        match["created_at"] = {"$gte": start, "$lt": end}
    elif date_sel == "last1h":
        start = now - timedelta(hours=1)
        match["created_at"] = {"$gte": start}
    elif date_sel == "all":
        pass
    else:
        # fallback -> today
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        match["created_at"] = {"$gte": start}

    if origin in {"live", "backfill"}:
        match["origin"] = origin

        # Map numeric (0/1/2) ya string ("pos"/"neg"/"neu") ko classes me
    pipeline = [
        {"$match": match},
        {
            "$project": {
                "sint": {"$cond": [{"$isNumber": "$sentiment"}, "$sentiment", None]},
                "sstr": {"$toLower": {"$ifNull": ["$sentiment", ""]}},
            }
        },
        {
            "$project": {
                "cls": {
                    "$switch": {
                        "branches": [
                            {"case": {"$in": ["$sint", [1]]}, "then": "pos"},
                            {
                                "case": {
                                    "$regexMatch": {
                                        "input": "$sstr",
                                        "regex": "^pos",
                                    }
                                },
                                "then": "pos",
                            },
                            {"case": {"$in": ["$sint", [2]]}, "then": "neu"},
                            {
                                "case": {
                                    "$regexMatch": {
                                        "input": "$sstr",
                                        "regex": "^neu",
                                    }
                                },
                                "then": "neu",
                            },
                            {"case": {"$in": ["$sint", [0]]}, "then": "neg"},
                            {
                                "case": {
                                    "$regexMatch": {
                                        "input": "$sstr",
                                        "regex": "^neg",
                                    }
                                },
                                "then": "neg",
                            },
                        ],
                        "default": "neg",
                    }
                }
            }
        },
        {"$group": {"_id": "$cls", "n": {"$sum": 1}}},
    ]

    buckets = {d["_id"]: d["n"] for d in col.aggregate(pipeline)}
    pos = int(buckets.get("pos", 0))
    neu = int(buckets.get("neu", 0))
    neg = int(buckets.get("neg", 0))
    total = pos + neu + neg

    # Rate per min: last 5 minutes, sirf origin filter apply karo
    since = now - timedelta(minutes=5)
    rate_match = {}
    if origin in {"live", "backfill"}:
        rate_match["origin"] = origin
    rate_match["created_at"] = {"$gte": since}

    rate_count = col.count_documents(rate_match)
    rate_per_min = round(rate_count / 5.0, 2)

    return JsonResponse(
        {"pos": pos, "neu": neu, "neg": neg, "total": total, "rate_per_min": rate_per_min}
    )


from django.shortcuts import redirect
from django.urls import reverse


def api_live_counts(request):
    # window seconds (default 60)
    win = int(request.GET.get("win", 60))
    since = dt.datetime.utcnow() - dt.timedelta(seconds=win)

    # totals
    total = col.estimated_document_count()
    last_win = col.count_documents({"processed_at": {"$gte": since}})

    # by sentiment
    agg_sent = list(col.aggregate([
        {"$match": {"processed_at": {"$gte": since}, "sentiment": {"$in": [0,1,2]}}},
        {"$group": {"_id": "$sentiment", "c": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]))
    by_sent = {str(d["_id"]): d["c"] for d in agg_sent}

    # by language
    agg_lang = list(col.aggregate([
        {"$match": {"processed_at": {"$gte": since}}},
        {"$group": {"_id": "$lang", "c": {"$sum": 1}}},
        {"$sort": {"c": -1}},
        {"$limit": 10}
    ]))
    by_lang = [{"lang": (d["_id"] or "unk"), "count": d["c"]} for d in agg_lang]

    return JsonResponse({
        "time": dt.datetime.utcnow().isoformat() + "Z",
        "window_sec": win,
        "total": total,
        "last_window": last_win,
        "by_sentiment": by_sent,
        "by_language": by_lang
    })

































