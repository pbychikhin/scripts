*** Important ***
ram_allocation_ratio calculations were tested against Queens
This is probably valid also for Rocky
Since Stein, initial_ram_allocation_ratio was added
And probably the mechanism for changing ratios dynamically over:
openstack resource provider inventory set <provider_uuid> --amend --resource MEMORY_MB:allocation_ratio=1.1
All of the above needs to be tested for releases after Queens

libvirt_memory_report is meant to run from Salt
libvirt_memory_report.py generates memory report on a host and outputs it in JSON/Base64 text
libvirt_memory_report_merge.py merges reports from hosts and creates aggregated report. It expects its input in JSON produced by Salt with static option (-s)
report_lib.py library used by libvirt_memory_report_merge.py
The command may look like:
sudo salt -C "E@^compute_hostname" --out json -s cmd.script https://raw.githubusercontent.com/pbychikhin/scripts/master/Linux/libvirt_memory_report.py | ./libvirt_memory_report_merge.py -x
