When sending over SSH, zfs_backup.py uses known_hosts.txt file to get check the SSH server's public key against known ones.
The format of the file is:
[hostname.domain.tld]:port_num algo key info
For instance, the record of a real server may look like:
[testbsd.local]:22 ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBB6VQ5/+gRK5RlhLNJNTws6N6URVrLcvJZG594a9RrgyWfQr6uPkA+sr56odqAQFc9F8EEFBMcUb0IVSKfXgTIY= root@testbsd.local
The file may contain more than one record. Host keys can be found in /etc/ssh/*.pub files.
The format of known_hosts.txt described above is the only format I've found so far. It might happen that the real format that is supported by SSH client implementations is more sophisticated.
