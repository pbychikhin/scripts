#!/usr/bin/env python3
from collections import namedtuple
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
        "mem_available": 0.0,
        "ratio": 0.0
    },
    "top_n_hosts_least_mem_available": list(),
    "top_n_procs_biggest_rss": list(),
    "mem_all_vms": {
        "memory": 0.0,
        "rss": 0.0,
        "ratio": 0.0
    },
    "top_n_vms_by_memory": list(),
    "top_n_vms_by_rss": list(),
    "top_n_vms_by_ratio": list()
}

hosts = list()
procs = list()
vms = list()
Host = namedtuple("Host", ["host", "mem_total", "mem_available", "ratio"])
Proc = namedtuple("Proc", ["host", "name", "rss"])
Vm = namedtuple("Vm", ["host", "uuid", "memory", "rss", "ratio"])

for hk, hv in host_report.items():
    report["mem_all_hosts"]["mem_total"] += hv["host"]["mem_total"]
    report["mem_all_hosts"]["mem_available"] += hv["host"]["mem_available"]
    hosts.append(Host(hk, hv["host"]["mem_total"], hv["host"]["mem_available"], hv["host"]["ratio"]))
    for pk, pv in hv["proc"].items():
        procs.append(Proc(hk, pv["name"], pv["rss"]))
    for vk, vv in hv["vm"].items():
        report["mem_all_vms"]["memory"] += vv["memory"]
        report["mem_all_vms"]["rss"] += vv["rss"]
        vms.append(Vm(hk, vk, vv["memory"], vv["rss"], vv["ratio"]))

report["mem_all_hosts"]["ratio"] = round(report["mem_all_hosts"]["mem_available"] / report["mem_all_hosts"]["mem_total"], 2)
report["mem_all_vms"]["ratio"] = round(report["mem_all_vms"]["rss"] / report["mem_all_vms"]["memory"], 2)
report["top_n_hosts_least_mem_available"] = sorted(hosts, key=lambda a: a.mem_available)
report["top_n_procs_biggest_rss"] = sorted(procs, key=lambda a: a.rss, reverse=True)
report["top_n_vms_by_memory"] = sorted(vms, key=lambda a: a.memory, reverse=True)
report["top_n_vms_by_rss"] = sorted(vms, key=lambda a: a.rss, reverse=True)
report["top_n_vms_by_ratio"] = sorted(vms, key=lambda a: a.ratio, reverse=True)

n = 10

print("Total/available memory across all hosts")
print("mem_total {mem_total}, mem_available {mem_available}, ratio {ratio}".format(**report["mem_all_hosts"]))
print("")
print("Top {} hosts with least memory available".format(n))
for i in report["top_n_hosts_least_mem_available"][:n]:
    print(i)
print("")
print("Top {} processes with biggest rss".format(n))
for i in report["top_n_procs_biggest_rss"][:n]:
    print(i)
print("")
print("Total memory/rss of all VMs across all hosts")
print("memory {memory}, rss {rss}, ratio {ratio}".format(**report["mem_all_vms"]))
print("")
print("Top {} VMs by memory".format(n))
for i in report["top_n_vms_by_memory"][:n]:
    print(i)
print("")
print("Top {} VMs by rss".format(n))
for i in report["top_n_vms_by_rss"][:n]:
    print(i)
print("")
print("Top {} VMs by ratio".format(n))
for i in report["top_n_vms_by_ratio"][:n]:
    print(i)
