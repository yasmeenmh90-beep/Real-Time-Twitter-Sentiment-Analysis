#!/usr/bin/env bash
LOGDIR="$HOME/Desktop/Thesis/Code/sentiment_realtime_project/twitter_scraper/logs"
mv "$LOGDIR/tweets.jsonl" "$LOGDIR/tweets-$(date +%Y-%m-%d).jsonl" 2>/dev/null || true
touch "$LOGDIR/tweets.jsonl"

