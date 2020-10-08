
import threading
import queue
from confluent_kafka import Consumer, OFFSET_END, OFFSET_BEGINNING, OFFSET_STORED
from time import time
from argparse import ArgumentParser

cmd = ArgumentParser(description="Load-test consumer")
cmd.add_argument("-t", metavar="name", help="Topic names (comma-separated), topic name can be given as Topic:num", required=True)
cmd.add_argument("-i", metavar="name", help="Consumer group id", required=True)
cmd.add_argument("-g", metavar="int", help="Max groups per topic (0 - unlimited (default))", type=int, default=0)
cmd.add_argument("-m", metavar="int", help="Number of messaget to consume at once (1)", type=int, default=1)
cmd.add_argument("-s", metavar="str", help="Kafka server, e.g kafka-host.domain.tld:443", required=True)
cmd.add_argument("-o", metavar="str", help="Initial offset (beginning, end, stored (default))", choices=("beginning", "end", "stored"), default="stored")
cmd.add_argument("--ca", metavar="str", help="CA file path")
cmd.add_argument("--user", metavar="str", help="User name")
cmd.add_argument("--password", metavar="str", help="User password")
cmd.add_argument("--verbose", help="Verbose messages", action="store_true", default=False)
cmdargs = cmd.parse_args()

cmdargs.t = sorted(x.strip() for x in cmdargs.t.split(","))
topics_expanded = list()
for t in cmdargs.t:
    t_parts = t.split(":")
    if len(t_parts) > 1:
        topics_expanded.extend([t_parts[0]] * int(t_parts[1]))
    else:
        topics_expanded.append(t_parts[0])
cmdargs.t = topics_expanded
stop = False


def on_assign_handler_factory(message_queue):
    def on_assign_handler(consumer, partitions):
        offset = {"beginning": OFFSET_BEGINNING,
                  "end": OFFSET_END,
                  "stored": OFFSET_STORED}
        for p in partitions:
            p.offset = offset[cmdargs.o]
            message_queue.put_nowait({"type": "notice",
                                     "message": "{}, {}: Consume from partition {}".format(threading.current_thread().name, p.topic, p.partition)})
        consumer.assign(partitions)
    return on_assign_handler


def on_revoke_handler_factory(message_queue):
    def on_revoke_handler(consumer, partitions):
        for p in partitions:
            message_queue.put_nowait({"type": "notice", "message": "{}, {}: Revoke from partition {}".format(threading.current_thread().name, p.topic, p.partition)})
    return on_revoke_handler


def consume(topic, message_queue, group_suffix):
    group_id = "-".join((cmdargs.i, group_suffix))
    consumer = Consumer({"group.id": group_id,
                         "bootstrap.servers": cmdargs.s,
                         "ssl.ca.location": cmdargs.ca,
                         "security.protocol": "SASL_SSL",
                         "sasl.mechanism": "PLAIN",
                         "sasl.username": cmdargs.user,
                         "sasl.password": cmdargs.password})
    message_queue.put_nowait({"type": "notice",
                              "message": "{}, {}: Consume as {}".format(threading.current_thread().name, topic, group_id)})
    consumer.subscribe([topic], on_assign_handler_factory(message_queue), on_revoke_handler_factory(message_queue))
    count = 0
    totalcount = 0
    timelast = time()
    while True:
        messages = consumer.consume(cmdargs.m, 1)
        count += len(messages)
        totalcount += len(messages)
        timecurrent = time()
        if timecurrent - timelast > 1:
            message_queue.put_nowait({"type": "count",
                                      "message": "{}, {}: Consumed {} messages, {} msg/sec".format(threading.current_thread().name, topic, totalcount, round(count / (timecurrent - timelast), 2)),
                                      "count": count,
                                      "time": timecurrent - timelast})
            timelast = timecurrent
            count = 0
            if stop:
                break
    consumer.close()


consumers = list()
consumer_messages = queue.Queue()
main_count = 0
main_totalcount = 0
main_time = 0
message_count = 0
try:
    i = 0
    topic_last = None
    for t in cmdargs.t:
        if topic_last != t:
            topic_last = t
            i = 0
        elif 0 < cmdargs.g == i:
            i = 0
        consumers.append(threading.Thread(target=consume, args=(t, consumer_messages, str(i))))
        i += 1
    for c in consumers:
        c.start()
    while True:
        try:
            message = consumer_messages.get(timeout=1)
            if message["type"] == "count":
                main_count += message["count"]
                main_totalcount += message["count"]
                message_count += 1
                main_time = max(main_time, message["time"])
            if cmdargs.verbose or message["type"] in ("notice",):
                print(message["message"])
            if message_count == len(consumers):
                print("### TOTAL: Consumed {} messages, {} msg/sec".format(main_totalcount, round((main_count / main_time), 2)))
                main_count = 0
                main_time = 0
                message_count = 0
        except queue.Empty:
            pass
        for c in consumers:
            if not c.is_alive():
                print("{} has died - Stop (wait for others)".format(c.name))
                stop = True
                break
        if stop:
            break
except KeyboardInterrupt:
    print("Stop (wait for consumers)")
    stop = True
for c in consumers:
    c.join()
