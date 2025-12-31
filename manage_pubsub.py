import sys
from google.cloud import pubsub_v1
from google.api_core.exceptions import AlreadyExists, NotFound


project_id = "pubsub-hajar"
suffix = "hajar_azimova"
levels = ["DEBUG", "INFO", "WARN", "ERROR", "ALERT"]
subscriber_ids = ["0", "1", "2", "3"]  


def topic_name(level):
    return f"{level}-{suffix}"

def subscription_name(sub_id, level):
    return f"sub{sub_id}-{level}-{suffix}"

def setup():
    pub = pubsub_v1.PublisherClient()
    sub = pubsub_v1.SubscriberClient()

    print("\nCreating topics...\n")
    for lvl in levels:
        t = topic_name(lvl)
        t_path = pub.topic_path(project_id, t)
        try:
            pub.create_topic(request={"name": t_path})
            print("Created topic:", t)
        except AlreadyExists:
            print("Topic exists:", t)

    print("\nCreating subscriptions...\n")
    for sid in subscriber_ids:
        for lvl in levels:
            s = subscription_name(sid, lvl)
            s_path = sub.subscription_path(project_id, s)
            t_path = pub.topic_path(project_id, topic_name(lvl))
            try:    
                sub.create_subscription(request={"name": s_path, "topic": t_path, "ack_deadline_seconds": 10})
                print("Created subscription:", s)
            except AlreadyExists:
                print("Subscription exists:", s)

    print("\nSETUP COMPLETED \n")

def teardown():
    pub = pubsub_v1.PublisherClient()
    sub = pubsub_v1.SubscriberClient()

    print("\nDeleting subscriptions...\n")
    for sid in subscriber_ids:
        for lvl in levels:
            s = subscription_name(sid, lvl)
            s_path = sub.subscription_path(project_id, s)
            try:
                sub.delete_subscription(request={"subscription": s_path})
                print("Deleted subscription:", s)
            except NotFound:
                print("Subscription not found:", s)

    print("\nDeleting topics...\n")
    for lvl in levels:
        t = topic_name(lvl)
        t_path = pub.topic_path(project_id, t)
        try:
            pub.delete_topic(request={"topic": t_path})
            print("Deleted topic:", t)
        except NotFound:
            print("Topic not found:", t)

    print("\nTEARDOWN COMPLETED \n")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python manage_pubsub.py setup | teardown")
        sys.exit(1)

    action = sys.argv[1].lower()
    if action == "setup":
        setup()
    elif action == "teardown":
        teardown()
    else:
        print("Unknown command. Use setup or teardown.")

