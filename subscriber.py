import argparse
import json
import os
import re
import threading
import time
from datetime import datetime

import yaml
from google.cloud import pubsub_v1

LEVELS = ["DEBUG", "INFO", "WARN", "ERROR", "ALERT"]

compiled_rules = {lvl: [] for lvl in LEVELS}
rules_mtime = 0


def load_rules_file(path):
    if path.endswith(".json"):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    else:
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)


def reload_rules_loop(rules_path, sub_id, reload_sec):
    global compiled_rules, rules_mtime

    while True:
        try:
            if os.path.exists(rules_path):
                mtime = os.path.getmtime(rules_path)

                
                if mtime != rules_mtime:
                    data = load_rules_file(rules_path) or {}
                    subs = data.get("subscribers", {})
                    rules_list = subs.get(str(sub_id), [])

                    new_rules = {lvl: [] for lvl in LEVELS}
                    for rule in rules_list:
                        lvl = str(rule.get("level", "")).upper().strip()
                        pat = str(rule.get("pattern", "")).strip()
                        if lvl in new_rules and pat:
                            try:
                                new_rules[lvl].append(re.compile(pat))
                            except re.error:
                                pass

                    compiled_rules = new_rules
                    rules_mtime = mtime
                    print(f"[RULES] subscriber {sub_id} reloaded at {datetime.now().strftime('%H:%M:%S')}")
        except Exception as e:
            print("[RULES] reload error:", e)

        time.sleep(reload_sec)


def match_pattern(level, text):
    """
    Returns the regex string that matched (pattern.pattern) or None
    """
    for pattern in compiled_rules.get(level, []):
        if pattern.search(text):
            return pattern.pattern
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", default="pubsub-hajar")
    ap.add_argument("--suffix", default="hajar_azimova")
    ap.add_argument("--id", required=True)         
    ap.add_argument("--rules", default="rules.yaml")
    ap.add_argument("--reload_sec", type=int, default=10)
    args = ap.parse_args()

    sub_id = str(args.id)


    threading.Thread(
        target=reload_rules_loop,
        args=(args.rules, sub_id, args.reload_sec),
        daemon=True
    ).start()

    subscriber = pubsub_v1.SubscriberClient()
    futures = []

  
    for level in LEVELS:
        subscription_id = f"sub{sub_id}-{level}-{args.suffix}"
        sub_path = subscriber.subscription_path(args.project, subscription_id)

        def make_callback(lvl):
            def callback(message):
                text = message.data.decode("utf-8", errors="replace")

                pat = match_pattern(lvl, text)
                if pat:
                    now = datetime.now().strftime("%H:%M:%S")
                    print(f"[SUB {sub_id}] [{now}] MATCH {lvl}: {text} | pattern={pat}\n", end="")


                message.ack()
            return callback

        futures.append(subscriber.subscribe(sub_path, callback=make_callback(level)))
        print(f"[SUB {sub_id}] listening: {subscription_id}")

    try:
        for f in futures:
            f.result()
    except KeyboardInterrupt:
        for f in futures:
            f.cancel()


if __name__ == "__main__":
    main()
