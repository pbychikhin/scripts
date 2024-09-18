#!/usr/bin/env python3

from subprocess import run, PIPE
from json import dumps
from base64 import b64encode
# from sys import stderr
from os import scandir, readlink
from os.path import basename

def get_vm_list():
    return run(["virsh", "list", "--all", "--uuid"], check=True, stdout=PIPE, universal_newlines=True).stdout.strip().splitlines()

def get_vm_info(vm_uuid):
    rv = {
        "pid": None,
        "rss": 0
    }
    rv_keys_map = {
        "Name": "name",
        "Max memory": "memory",
        "rss": "rss"
    }
    rv_float_items = ["memory", "rss"]
    rv_keys = ["Name", "Max memory"]
    for line in run(["virsh", "dominfo", vm_uuid], check=True, stdout=PIPE, universal_newlines=True).stdout.strip().splitlines():
        line_parts = line.split(":")
        if line_parts[0] in rv_keys:
            rv[rv_keys_map[line_parts[0]]] = line_parts[1].strip().split()[0]
    try:
        with open("/var/run/libvirt/qemu/{}.pid".format(rv["name"])) as qemu_pid_file:
            for line in qemu_pid_file:
                rv["pid"] = line.strip()
                break
    except FileNotFoundError:
        pass
    if rv["pid"] is not None:
        rv_keys = ["rss"]
        for line in run(["virsh", "dommemstat", vm_uuid], check=True, stdout=PIPE, universal_newlines=True).stdout.strip().splitlines():
            line_parts = line.split()
            if line_parts[0] in rv_keys:
                rv[rv_keys_map[line_parts[0]]] = line_parts[1].strip().split()[0]
    for k in rv_float_items:
        rv[k] = float(rv[k])
    return rv

def get_proc_list():
    rv = list()
    for i in scandir("/proc"):
        if i.is_dir() and i.name.isdecimal():
            rv.append(i.name)
    return rv

def get_proc_info(pid):
    rv = dict()
    try:
        rv["name"] = basename(readlink("/proc/{}/exe".format(pid)))
        with open("/proc/{}/statm".format(pid)) as proc_statm_file:
            for line in proc_statm_file:
                rv["rss"] = float(line.strip().split()[1]) * 4
                break
        with open("/proc/{}/comm".format(pid)) as proc_comm_file:
            for line in proc_comm_file:
                rv["comm"] = line.strip()
                break
        return rv
    except FileNotFoundError:
        return None

def get_host_info():
    rv = dict()
    rv_keys_map = {
        "MemTotal": "mem_total",
        "MemAvailable": "mem_available",
    }
    with open("/proc/meminfo") as meminfo_file:
        for line in meminfo_file:
            line_parts = line.strip().split(":")
            if line_parts[0] in rv_keys_map.keys():
                rv[rv_keys_map[line_parts[0]]] = float(line_parts[1].split()[0])
    return rv

report = {
    "host": dict(),
    "proc": dict(),
    "vm": dict()
}

for vm in get_vm_list():
    vm_info = get_vm_info(vm)
    ratio = round(vm_info["rss"] / vm_info["memory"], 2)
    report["vm"][vm] = {
        "pid": vm_info["pid"],
        "memory": round(vm_info["memory"] / 1024, 2),
        "rss": round(vm_info["rss"] / 1024, 2),
        "ratio": ratio
    }

for proc in set(get_proc_list()) - set([x["pid"] for x in report["vm"].values()]):
    proc_info = get_proc_info(proc)
    if proc_info is not None:
        report["proc"][proc] = {
            "name": proc_info["name"] if proc_info["name"].find(proc_info["comm"]) == 0 else "{}:{}".format(proc_info["name"], proc_info["comm"]),
            "rss": round(proc_info["rss"] / 1024, 2)
        }

host_info = get_host_info()
report["host"] = {
    "mem_total": round(host_info["mem_total"] / 1024, 2),
    "mem_available": round(host_info["mem_available"] / 1024, 2),
}

print(b64encode(dumps(report).encode()).decode())
