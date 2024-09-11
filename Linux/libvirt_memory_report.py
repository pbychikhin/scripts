#!/usr/bin/env python3

from subprocess import run, PIPE
from json import dumps
from base64 import b64encode

def get_vm_list():
    return run(["virsh", "list", "--uuid"], check=True, stdout=PIPE, universal_newlines=True).stdout.strip().splitlines()

def get_vm_mem_kb(vm_uuid):
    for line in run(["virsh", "dominfo", vm_uuid], check=True, stdout=PIPE, universal_newlines=True).stdout.strip().splitlines():
        line_parts = line.split(":")
        if line_parts[0] == "Used memory":
            return float(line_parts[1].strip().split()[0]) * 1.024

def get_vm_pid(vm_uuid):
    return run(["pgrep", "-f", vm_uuid], check=True, stdout=PIPE, universal_newlines=True).stdout.strip().splitlines()[0]

def get_proc_mem_kb(pid):
    with open("/proc/{}/status".format(pid)) as proc_status:
        for line in proc_status:
            line_parts = line.split(":")
            if line_parts[0] == "VmRSS":
                return float(line_parts[1].strip().split()[0])

report = dict()
for vm in get_vm_list():
    report[vm] = {
        "vm_mem": get_vm_mem_kb(vm),
        "proc_mem": get_proc_mem_kb(get_vm_pid(vm))
    }

print(b64encode(dumps(report).encode()).decode())
