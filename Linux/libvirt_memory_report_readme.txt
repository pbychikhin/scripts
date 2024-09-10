libvirt_memory_report is meant to run from Salt
libvirt_memory_report.py generates memory report on a host and outputs it in JSON/Base64 text
libvirt_memory_report_merge.py merges reports from hosts and creates aggregated report. It expects its input in JSON produced by Salt with static option (-s)
The command may look like:
sudo salt -C "E@^compute_hostname" --out json -s cmd.script https://raw.githubusercontent.com/pbychikhin/scripts/master/Linux/libvirt_memory_report.py | ./libvirt_memory_report_merge.py -x libvirt_memory_report_$(date +%m%d%H%M).xlsx
