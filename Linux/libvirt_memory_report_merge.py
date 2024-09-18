#!/usr/bin/env python3

from json import load, loads
from base64 import b64decode
from sys import stdin

host_report = dict()

for k, v in load(stdin).items():
    if v["retcode"] == 0:
        host_report[k] = loads(b64decode(v["stdout"]).decode())

report = {
    "mem_all_hosts": {
        "mem_total": 0.0,
        "mem_available": 0.0
    },
    "top_n_hosts_least_mem_available": list(),
    "top_n_procs_biggest_rss": list(),
    "mem_all_vms": {
        "memory": 0.0,
        "rss": 0.0,
        "ratio": None
    },
    "top_n_vms_by_memory": list(),
    "top_n_vms_by_rss": list(),
    "top_n_vms_by_ratio": list()
}

hosts = list()
procs = list()
vms = list()

for host, host_data in host_report.items():
    report["mem_all_hosts"]["mem_total"] += host_data["host"]["mem_total"]
    report["mem_all_hosts"]["mem_available"] += host_data["host"]["mem_available"]
    hosts.append({
        "host": host,
        "mem_total": host_data["host"]["mem_total"],
        "mem_available": host_data["host"]["mem_available"],
    })

# sorted_keys = sorted(list(report.keys()), key=lambda a: report[a]["ratio"], reverse=True)
# for k in sorted_keys:
#     print("{}: (actual) {actual:.2f}, (rss) {rss:.2f}, (proc_rss) {proc_rss:.2f},(ratio) {ratio:.2f}".format(k, **report[k]))
