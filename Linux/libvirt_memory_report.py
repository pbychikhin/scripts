#!/usr/bin/env python3

from subprocess import run, PIPE, CalledProcessError
from json import dumps
from base64 import b64encode
# from sys import stderr
from os import scandir, readlink
from os.path import basename
from configparser import ConfigParser

def get_vm_list():
    try:
        return run(["virsh", "list", "--all", "--uuid"], check=True, stdout=PIPE, universal_newlines=True).stdout.strip().splitlines()
    except FileNotFoundError:
        return list()

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

def get_service_list():
    return [line.strip().split()[0] for line in run(["systemctl", "list-units", "-t", "service", "--plain", "--no-pager", "--no-legend", "--full", "--lines", "0"],
                                                    check=True, stdout=PIPE, universal_newlines=True).stdout.strip().splitlines()]

def get_proc_service(pid, service_list):
    try:
        run_rv = run(["systemctl", "status", pid, "--plain", "--no-pager", "--no-legend", "--full", "--lines", "0"], check=True, stdout=PIPE, universal_newlines=True)
    except CalledProcessError:
        return None
    service = run_rv.stdout.strip().splitlines()[0].strip().split()[1]
    return service if service in service_list else None

def get_proc_info(pid, service_list):
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
        rv["service"] = get_proc_service(pid, service_list)
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
    nova_conf = ConfigParser(strict=False)
    nova_conf.read("/etc/nova/nova.conf")
    rv["ram_allocation_ratio"] = nova_conf.getfloat("DEFAULT", "ram_allocation_ratio", fallback=0.0)
    rv["reserved_host_memory_mb"] = nova_conf.getint("DEFAULT", "reserved_host_memory_mb", fallback=512)
    return rv

report = {
    "host": dict(),
    "proc": dict(),
    "vm": dict()
}

for vm in get_vm_list():
    vm_info = get_vm_info(vm)
    report["vm"][vm] = {
        "pid": vm_info["pid"],
        "memory": vm_info["memory"] / 1024,
        "rss": vm_info["rss"] / 1024,
        "ratio": vm_info["rss"] / vm_info["memory"]
    }

service_list = get_service_list()

for proc in set(get_proc_list()) - set([x["pid"] for x in report["vm"].values()]):
    proc_info = get_proc_info(proc, service_list)
    if proc_info is not None:
        report["proc"][proc] = {
            "name": proc_info["name"] if proc_info["name"].find(proc_info["comm"]) == 0 else "{}:{}".format(proc_info["name"], proc_info["comm"]),
            "rss": proc_info["rss"] / 1024
        }
        if proc_info["service"] is not None:
            report["proc"][proc]["name"] = "{}:{}".format(proc_info["service"], report["proc"][proc]["name"])

host_info = get_host_info()
report["host"] = {
    "mem_total": host_info["mem_total"] / 1024,
    "mem_available": host_info["mem_available"] / 1024,
    "ratio": host_info["mem_available"] / host_info["mem_total"],
    "ram_allocation_ratio": host_info["ram_allocation_ratio"],
    "reserved_host_memory_mb": host_info["reserved_host_memory_mb"]
}

print(b64encode(dumps(report).encode()).decode())
