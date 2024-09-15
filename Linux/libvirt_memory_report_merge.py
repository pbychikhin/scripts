#!/usr/bin/env python3

from json import load, loads
from base64 import b64decode
from sys import stdin

report = dict()

for k, v in load(stdin).items():
    if v["retcode"] == 0:
        report.update(loads(b64decode(v["stdout"]).decode()))

sorted_keys = sorted(list(report.keys()), key=lambda a: report[a]["ratio"], reverse=True)
for k in sorted_keys:
    print("{}: (actual) {actual:.2f}, (rss) {rss:.2f}, (proc_rss) {proc_rss:.2f},(ratio) {ratio:.2f}".format(k, **report[k]))
