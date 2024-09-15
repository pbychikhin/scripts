#!/usr/bin/env python3

from subprocess import run, PIPE
from json import dumps
from base64 import b64encode
from sys import stderr

def get_vm_list():
    return run(["virsh", "list", "--uuid"], check=True, stdout=PIPE, universal_newlines=True).stdout.strip().splitlines()

def get_vm_mem_kb(vm_uuid):
    rv = dict()
    rv_keys = ["actual", "rss"]
    print("DEBUG, virsh dommemstat {}".format(vm_uuid), file=stderr)
    for line in run(["virsh", "dommemstat", vm_uuid], check=True, stdout=PIPE, universal_newlines=True).stdout.strip().splitlines():
        line_parts = line.split()
        if line_parts[0] in rv_keys:
            rv[line_parts[0]] = float(line_parts[1].strip().split()[0])
    print("DEBUG, {}".format(rv), file=stderr)
    return rv

def get_vm_pid(vm_uuid):
    return run(["pgrep", "-f", vm_uuid], check=True, stdout=PIPE, universal_newlines=True).stdout.strip().splitlines()[0]

def get_proc_mem_kb(pid):
    rv = dict()
    rv_keys = ["VmRSS"]
    with open("/proc/{}/status".format(pid)) as proc_status:
        for line in proc_status:
            line_parts = line.split(":")
            if line_parts[0] in rv_keys:
                rv[line_parts[0]] = float(line_parts[1].strip().split()[0])
    return rv

report = dict()
for vm in get_vm_list():
    vm_mem = get_vm_mem_kb(vm)
    proc_mem = get_proc_mem_kb(get_vm_pid(vm))
    ratio = vm_mem["rss"] / vm_mem["actual"]
    report[vm] = {
        "actual": vm_mem["actual"] / 1024,
        "rss": vm_mem["rss"] / 1024,
        "proc_rss": proc_mem["VmRSS"] / 1024,
        "ratio": ratio
    }

print(b64encode(dumps(report).encode()).decode())
