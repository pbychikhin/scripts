#!/usr/bin/env python3

from json import load, loads
from base64 import b64decode
from sys import stdin

report = dict()

for k, v in load(stdin).items():
    if v["retcode"] == 0:
        report.update(loads(b64decode(v["stdout"]).decode()))

for k, v in report.items():
    print("{}: (vm_mem) {vm_mem}, (proc_mem) {proc_mem}, (ratio) {ratio}".format(k, **v))
