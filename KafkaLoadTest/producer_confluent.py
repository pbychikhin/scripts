
from confluent_kafka import Producer
from time import time
import string
import random
import threading
import queue
from argparse import ArgumentParser

cmd = ArgumentParser(description="Load-test producer")
cmd.add_argument("-z", metavar="num", help="Size of message, bytes (2000)", default=2000, type=int)
cmd.add_argument("-t", metavar="name", help="Topic names (comma-separated), topic name can be given as Topic:num", required=True)
cmd.add_argument("-c", metavar="num", help="Number of messages before flush (1000)", default=1000, type=int)
cmd.add_argument("-s", metavar="str", help="Kafka server, e.g kafka-host.domain.tld:443", required=True)
cmd.add_argument("--ca", metavar="str", help="CA file path")
cmd.add_argument("--user", metavar="str", help="User name")
cmd.add_argument("--password", metavar="str", help="User password")
cmd.add_argument("--verbose", help="Verbose messages", action="store_true", default=False)
cmdargs = cmd.parse_args()

cmdargs.t = (x.strip() for x in cmdargs.t.split(","))
topics_expanded = list()
for t in cmdargs.t:
    t_parts = t.split(":")
    if len(t_parts) > 1:
        topics_expanded.extend([t_parts[0]] * int(t_parts[1]))
    else:
        topics_expanded.append(t_parts[0])
cmdargs.t = topics_expanded
stop = False

print("Generate messages")
messages = [bytes(''.join(random.choices(string.ascii_uppercase + string.digits, k=cmdargs.z)), "ascii") for x in range(1, 1000)]


def produce(topic, message_queue):
    producer = Producer({"bootstrap.servers": cmdargs.s,
                     "ssl.ca.location": cmdargs.ca,
                     "security.protocol": "SASL_SSL",
                     "sasl.mechanism": "PLAIN",
                     "sasl.username": cmdargs.user,
                     "sasl.password": cmdargs.password})
    message_queue.put_nowait({"type": "notice",
                              "message": "{}, {}: Produce".format(threading.current_thread().name, topic)})
    count = 0
    totalcount = 0
    timelast = time()
    while True:
        if stop:
            break
        try:
            producer.produce(topic, random.choice(messages))
        except BufferError:
            producer.flush()
        count += 1
        totalcount += 1
        if count > cmdargs.c:
            producer.flush()
        timecurrent = time()
        if timecurrent - timelast > 1:
            message_queue.put_nowait({"type": "count",
                                      "message": "{}, {}: Produced {} messages, {} msg/sec".format(threading.current_thread().name, topic, totalcount, round(count / (timecurrent - timelast), 2)),
                                      "count": count,
                                      "time": timecurrent - timelast})
            timelast = timecurrent
            count = 0


producers = list()
producer_messages = queue.Queue()
main_count = 0
main_totalcount = 0
main_time = 0
message_count = 0
try:
    for t in cmdargs.t:
        producers.append(threading.Thread(target=produce, args=(t, producer_messages)))
    for p in producers:
        p.start()
    while True:
        try:
            message = producer_messages.get(timeout=1)
            if message["type"] == "count":
                main_count += message["count"]
                main_totalcount += message["count"]
                message_count += 1
                main_time = max(main_time, message["time"])
            if cmdargs.verbose or message["type"] in ("notice",):
                print(message["message"])
            if message_count == len(producers):
                print("### TOTAL: Produced {} messages, {} msg/sec".format(main_totalcount, round((main_count / main_time), 2)))
                main_count = 0
                main_time = 0
                message_count = 0
        except queue.Empty:
            pass
        for p in producers:
            if not p.is_alive():
                print("{} has died - Stop (wait for others)".format(p.name))
                stop = True
                break
        if stop:
            break
except KeyboardInterrupt:
    print("Stop (wait for producers)")
    stop = True
for p in producers:
    p.join()
