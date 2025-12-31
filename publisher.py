import argparse
import csv
import time
from google.cloud import pubsub_v1

LEVELS = ["DEBUG", "INFO", "WARN", "ERROR", "ALERT"]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", default="pubsub-hajar")
    ap.add_argument("--suffix", default="hajar_azimova")
    ap.add_argument("--csv", default="logs.csv")
    ap.add_argument("--sleep", type=int, default=2)
    args = ap.parse_args()

    publisher = pubsub_v1.PublisherClient()

   
    rows = []
    with open(args.csv, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)  
        for r in reader:
         
            if len(r) < 3:
                continue
            ts = r[0].strip()
            level = r[1].strip().upper()
            msg = r[2].strip()
            if not ts or level not in LEVELS or not msg:
                continue
            rows.append((ts, level, msg))

    if not rows:
        print("No valid rows found in logs.csv")
        return

    print(f"Loaded {len(rows)} log rows. Publisher started...")

    i = 0
    while True:
        ts, level, msg = rows[i]
        topic_id = f"{level}-{args.suffix}"
        topic_path = publisher.topic_path(args.project, topic_id)

      
        data = msg.encode("utf-8")
        publisher.publish(topic_path, data=data)

        now = time.strftime("%H:%M:%S")
        print(f"[PUB {now}] -> {topic_id}: {msg}")

        i = (i + 1) % len(rows)  
        time.sleep(args.sleep)   

if __name__ == "__main__":
    main()
