
import curses
import time
import random
import requests
import os
import sys
import uuid
import json
from collections import OrderedDict


# logfile = open("queue_delivery_stats.py.log", "w")
#
#
# def logmsg(msg):
#     print(msg, file=logfile)


class Relay:
    def __init__(self, keys):
        self.keys = keys
        self.on = False

    def press(self, key):
        if key in self.keys:
            if self.on:
                self.on = False
            else:
                self.on = True

    @property
    def off(self):
        return not self.on


class Queues:
    def __init__(self, from_api: list, vhost: str, sort_key="publish"):
        self.id = uuid.uuid4().int
        self.filtered = list()
        self.by_name = dict()
        self.diffs = dict()
        for q in from_api:
            if q["vhost"] == vhost and "message_stats" in q and {"publish", "deliver", "redeliver"} <= set(q["message_stats"].keys()):
                self.filtered.append(q)
                self.by_name[q["name"]] = q
        self.filtered.sort(key=lambda x: x["message_stats"][sort_key], reverse=True)

    def __sub__(self, other):
        if other.id not in self.diffs:
            self.diffs[other.id] = list()
            for q in self.filtered:
                other_q = other.by_name.get(q["name"], {"message_stats": {"publish": 0, "deliver": 0, "redeliver": 0}})
                self.diffs[other.id].append({
                    "name": q["name"],
                    "publish": q["message_stats"]["publish"] - other_q["message_stats"]["publish"],
                    "deliver": q["message_stats"]["deliver"] - other_q["message_stats"]["deliver"],
                    "redeliver": q["message_stats"]["redeliver"] - other_q["message_stats"]["redeliver"]
                })
            self.diffs[other.id].sort(key=lambda x: abs(x["publish"]), reverse=True)
        return self.diffs[other.id]

    def __iter__(self):
        return self.filtered.__iter__()


# def display_stats(q_2: Queues, q_1: Queues, stdscr: curses.window, start_line: int):
#     if q_2 and q_1:
#         i = start_line
#         for j in (q_2 - q_1):
#             stdscr.addstr(i, 0, "{name}: {publish}/{deliver}".format(**j))
#             i += 1
#             if i == 10:
#                 break

def display_stats(q_2: Queues, q_1: Queues, stdscr: curses.window, start_line: int):
    if q_2:
        i = start_line
        for j in q_2:
            stdscr.addstr(i, 0, "{name}: {publish}/{redeliver}".format(**j, **j["message_stats"]))
            i += 1
            if i == 25:
                break


def main_f(stdscr: curses.window):
    rc_params = {
        "url": os.environ["RABBIT_URL"],
        "user": os.environ["RABBIT_USER"],
        "pass": os.environ["RABBIT_PASS"],
        "vhost": os.environ["RABBIT_VHOST"]
    }
    curses.curs_set(0)
    q_1, q_2, old_q_1, old_q_2, t_1, t_2 = None, None, None, None, None, None
    pause = Relay(("p", "P"))
    run_count = 0
    stdscr.nodelay(True)
    while True:
        run_count += 1
        try:
            pause.press(stdscr.getkey())
        except curses.error:
            pass
        curses.flushinp()
        stdscr.erase()
        if pause.off:
            display_stats(q_2, q_1, stdscr, 1)
            old_q_2 = q_2
            old_q_1 = q_1
        else:
            display_stats(old_q_2, old_q_1, stdscr, 1)
        stdscr.refresh()
        if pause.on:
            stdscr.addstr(0, 0, "{} Paused (press p for resume)".format(run_count))
            stdscr.nodelay(False)
        else:
            if run_count < 3:
                stdscr.addstr(0, 0, "{} Initializing (press p for pause)".format(run_count))
            else:
                stdscr.addstr(0, 0, "{} Working (press p for pause)".format(run_count))
            stdscr.refresh()
            stdscr.nodelay(True)
            r = requests.get("/".join((rc_params["url"], "queues")), auth=(rc_params["user"], rc_params["pass"]))
            r.raise_for_status()
            q_1 = q_2
            t_1 = t_2
            q_2 = Queues(r.json(), rc_params["vhost"], "redeliver")
            t_2 = time.perf_counter()


if __name__ == "__main__":
    params = {
        "url": os.environ["RABBIT_URL"],
        "user": os.environ["RABBIT_USER"],
        "pass": os.environ["RABBIT_PASS"]
    }
    try:
        curses.wrapper(main_f)
    except KeyboardInterrupt:
        pass

