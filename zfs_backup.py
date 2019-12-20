
import subprocess
import gzip
import sys
import os
import os.path
import datetime
import paramiko
import logging
import io
import re
import stat
import argparse
import smtplib
import email.utils
from email.message import EmailMessage
from pathlib import Path
from datetime import datetime, timedelta

_NOW = datetime.today().strftime("%Y%m%d_%H%M%S")


def getLogger(dsuffix=None, stream=None):
    """
    Prepares logging facility
    :param dsuffix: a distinguishing suffix
    :param stream: an additional stream to log to. if set, the console out will also be written to a stream
    :return: logger object to be used in the rest of subs
    """
    log_formatter_stream = logging.Formatter(fmt="{asctime} {message}", style="{")
    log_formatter_file = logging.Formatter(fmt="{asctime} [{threadName}] [{levelname}] {message}", style="{")
    log_handler_stream = logging.StreamHandler()
    log_handler_stream.setLevel(logging.INFO)
    log_handler_stream.setFormatter(log_formatter_stream)
    log_handler_stream2 = None
    if stream is not None:
        log_handler_stream2 = logging.StreamHandler(stream)
        log_handler_stream2.setLevel(logging.INFO)
        log_handler_stream2.setFormatter(log_formatter_stream)
    if dsuffix is not None: dsuffix = dsuffix.strip()
    if dsuffix is not None and len(dsuffix) > 0:
        log_handler_file = logging.FileHandler(Path(sys.argv[0]).
                                               with_name(Path(sys.argv[0]).stem + "_" + dsuffix).
                                               with_suffix(".log").as_posix(), mode="w")
    else:
        log_handler_file = logging.FileHandler(Path(sys.argv[0]).with_suffix(".log").as_posix(), mode="w")
    log_handler_file.setLevel(logging.DEBUG)
    log_handler_file.setFormatter(log_formatter_file)
    log_logger = logging.getLogger(Path(sys.argv[0]).name)
    log_logger.addHandler(log_handler_stream)
    log_logger.addHandler(log_handler_file)
    if log_handler_stream2 is not None:
        log_logger.addHandler(log_handler_stream2)
    log_logger.setLevel(logging.DEBUG)
    return log_logger


def paramiko_log_to_file(filename, level=logging.DEBUG):
    """send paramiko logs to a logfile,
    if they're not already going somewhere"""
    logger = logging.getLogger("paramiko")
    if len(logger.handlers) > 0:
        return
    logger.setLevel(level)
    f = open(filename, "w")
    handler = logging.StreamHandler(f)
    frm = "%(levelname)-.3s [%(asctime)s.%(msecs)03d] thr=%(_threadid)-3d"
    frm += " %(name)s: %(message)s"
    handler.setFormatter(logging.Formatter(frm, "%Y%m%d-%H:%M:%S"))
    logger.addHandler(handler)


class ZfsError(RuntimeError):
    pass


class MyRuntimeError(RuntimeError):
    pass


class Zfs:
    def __init__(self, log):
        self.log = log

    def run_cmd(self, cmd):
        self.log.debug(">>> Run zfs cmd: {}".format(cmd))
        cmd_rv = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="ascii")
        if cmd_rv.returncode != 0:
            raise ZfsError("Zfs command error occurred.\n"
                           "Stdout: {}\n"
                           "Stderr: {}".format(cmd_rv.stdout, cmd_rv.stderr))
        else:
            return cmd_rv.stdout.split(sep="\n"), cmd_rv.stderr.split(sep="\n")

    def snapshot_exists(self, snapshot):
        self.log.debug("Checking if snapshot {} exists".format(snapshot))
        for item in self.run_cmd(("zfs", "list", "-t", "snapshot", "-o", "name"))[0]:
            if item == snapshot:
                self.log.debug("Snapshot {} exists".format(snapshot))
                return True
        self.log.debug("Snapshot {} doesn't exist".format(snapshot))
        return False

    def snapshot_create(self, snapshot):
        self.log.debug("Creating ZFS snapshot {}".format(snapshot))
        self.run_cmd(("zfs", "snapshot", snapshot))

    def snapshot_delete(self, snapshot):
        self.log.debug("Deleting ZFS snapshot {}".format(snapshot))
        if self.snapshot_exists(snapshot):
            self.run_cmd(("zfs", "destroy", snapshot))
        else:
            raise ZfsError("Snapshot {} doesn't exist".format(snapshot))

    def snapshot_send(self, snapshot, file, fileframework):
        self.log.info("Sending ZFS snapshot to the file {}".format(file))
        self.snapshot_create(snapshot)
        error = False
        try:
            with fileframework.open(file) as fileobj:
                with gzip.GzipFile(fileobj=fileobj, mode="wb", compresslevel=6) as out_file:
                    with subprocess.Popen(("zfs", "send", snapshot), stdout=subprocess.PIPE) as send_proc:
                        while True:
                            data = send_proc.stdout.read(1048576)
                            if data:
                                out_file.write(data)
                            else:
                                break
        except Exception:
            error = True
            self.log.exception("Exception occured while sending ZFS snapshot")
            try:
                self.log.debug("Cleaning up from file {} if exists".format(file))
                fileframework.stat(file)
            except Exception:
                pass
            else:
                fileframework.remove(file)
        finally:
            self.snapshot_delete(snapshot)
        if error:
            raise MyRuntimeError()


class RegularOutFile:
    @staticmethod
    def open(filename):
        return open(filename, "wb")

    @staticmethod
    def stat(filename):
        return os.stat(filename)

    @staticmethod
    def remove(filename):
        os.remove(filename)


class SFTPOutfile:
    def __init__(self, sftp):
        self.sftp = sftp

    def open(self, filename):
        return self.sftp.open(filename, "wb")

    def stat(self, filename):
        return self.sftp.stat(filename)

    def remove(self, filename):
        self.sftp.remove(filename)


def RegularRemoveOld(path, name, prefix, rttime, simulate, log):
    """
    Removes old files in a local directory
    :param path: Path to the backup directory
    :param name: Base file name
    :param prefix: Distinguishing prefix
    :param rttime: Retention time (timedelta)
    :param simulate: Simulate file removal - just log a message without real removing
    :param log: logger obj
    :return: a tuple: (Total items number, Removed items number)
    """
    item_glob_pattern = "{prefix}_{name}_????????_??????.{suffix}"  # a pattern of backup files
    error = False
    try:
        name_parts = dict(zip(("prefix", "name", "timestamp", "suffix"),
                              re.search("({})_(\S*)_(\d{{8}}_\d{{6}})\.(\S+)".format(prefix), name).groups()))
    except Exception:
        log.exception("Could not find valid name parts in {}".format(name))
        raise MyRuntimeError()
    try:
        name_time = datetime.strptime(name_parts["timestamp"], "%Y%m%d_%H%M%S")
    except Exception:
        log.exception("Could not convert {} to time".format(name_parts["timestamp"]))
        raise MyRuntimeError()
    log.debug("Performing clean up for {}".format(item_glob_pattern.format(**name_parts)))
    count = {"total": 0, "removed": 0}
    for item in Path(path).glob(item_glob_pattern.format(**name_parts)):
        if item.name == name:
            log.debug("Skipping this session's file {}".format(item.as_posix()))
            continue
        count["total"] += 1
        try:
            item_parts = dict(zip(("prefix", "name", "timestamp", "suffix"),
                              re.search("({})_(\S*)_(\d{{8}}_\d{{6}})\.(\S+)".format(prefix),
                                        item.as_posix()).groups()))
            item_time = datetime.strptime(item_parts["timestamp"], "%Y%m%d_%H%M%S")
        except Exception:
            log.debug("Skipping irrelevant file {}".format(item.as_posix()))
            continue
        if name_time - item_time > rttime:
            log.debug("Removing {}".format(item.as_posix()))
            try:
                if not simulate:
                    item.unlink()
                else:
                    log.debug("Simulating removal of old file {}".format(item.as_posix()))
            except Exception:
                error = True
                log.exception("Could not remove {}".format(item.as_posix()))
            else:
                count["removed"] += 1
        else:
            log.debug("Skipping old file {}, time delta is {} - too recent".format(item.as_posix(),
                                                                                   name_time - item_time))
    if error:
        raise MyRuntimeError()
    else:
        return count


def SFTPRemoveOld(sftp, path, name, prefix, rttime, simulate, log):
    """
    Removes old files in a local directory
    :param sftp: sftp client obj
    :param path: Path to the backup directory
    :param name: Base file name
    :param prefix: Distinguishing prefix
    :param rttime: Retention time (timedelta)
    :param simulate: Simulate file removal - just log a message without real removing
    :param log: logger obj
    :return: a tuple: (Total items number, Removed items number)
    """
    error = False
    try:
        name_parts = dict(zip(("prefix", "name", "timestamp", "suffix"),
                              re.search("({})_(\S*)_(\d{{8}}_\d{{6}})\.(\S+)".format(prefix), name).groups()))
    except Exception:
        log.exception("Could not find valid name parts in {}".format(name))
        raise MyRuntimeError()
    try:
        name_time = datetime.strptime(name_parts["timestamp"], "%Y%m%d_%H%M%S")
    except Exception:
        log.exception("Could not convert {} to time".format(name_parts["timestamp"]))
        raise MyRuntimeError()
    log.info("Performing clean up for {}".format("{prefix}_{name}_????????_??????.{suffix}".format(**name_parts)))
    count = {"total": 0, "removed": 0}
    for item in sftp.listdir_attr(path):
        if item.filename == name:
            log.debug("Skipping this session's file {}".format(item.filename))
            continue
        count["total"] += 1
        try:
            item_parts = dict(zip(("prefix", "name", "timestamp", "suffix"),
                                  re.search("({})_(\S*)_(\d{{8}}_\d{{6}})\.(\S+)".format(prefix),
                                            item.filename).groups()))
            item_time = datetime.strptime(item_parts["timestamp"], "%Y%m%d_%H%M%S")
        except Exception:
            log.debug("Skipping irrelevant file {}".format(item.filename))
            continue
        if not stat.S_ISREG(item.st_mode):
            log.debug("Skipping not-a-file {}".format(item.filename))
            continue
        if name_time - item_time > rttime:
            log.debug("Removing {}".format(item.filename))
            try:
                if not simulate:
                    sftp.remove(os.path.join(path, item.filename))
                else:
                    log.debug("Simulating removal of old file {}".format(item.filename))
            except Exception:
                error = True
                log.exception("Could not remove {}".format(item.filename))
            else:
                count["removed"] += 1
        else:
            log.debug("Skipping old file {}, time delta is {} - too recent".format(item.filename, name_time - item_time))
    if error:
        raise MyRuntimeError()
    else:
        return count


def getTimeDelta(timestr, log):
    """
    Returns a timedelta obj extracted from the input str
    :param timestr: time string, WeeksDaysHoursMinutes, IwJdKhLm
    :param log: logger obj
    :return: a timedelta obj
    """
    rv = None
    try:
        time_parts = tuple(int(x[0:-1]) for x in re.search("^(\d+w)?\s*(\d+d)?\s*(\d+h)?\s*(\d+m)?$", timestr.strip())
                           .groups(default="0t"))
        rv = timedelta(**dict(zip(("weeks", "days", "hours", "minutes"), time_parts)))
    except Exception:
        log.exception("Could not extract a time delta from {}".format(timestr))
        raise MyRuntimeError()
    return rv


if __name__ == "__main__":
    defaults = {
        "prefix": "zfsbackup",
        "known_hosts": "known_hosts{}.txt",
        "paramiko_log": "paramiko_log{}.log",
        "ssh_port": 22,
        "smtp_port": 25
    }
    cmd = argparse.ArgumentParser(description="Backups a ZFS dataset and stores it to a local folder or "
                                              "sends it over SSH to a remote folder")
    cmd.add_argument("-sshhost", metavar="name", help="SSH host name")
    cmd.add_argument("-sshport", metavar="number", help="SSH port number ({})".format(defaults["ssh_port"]),
                     type=int, default=defaults["ssh_port"])
    cmd.add_argument("-sshuser", metavar="name", help="SSH user name")
    cmd.add_argument("-sshpass", metavar="text", help="SSH user password")
    cmd.add_argument("-smtphost", metavar="name", help="SMTP host name")
    cmd.add_argument("-smtpport", metavar="number", help="SMTP port number ({})".format(defaults["smtp_port"]),
                     type=int, default=defaults["smtp_port"])
    cmd.add_argument("-smtpfrom", metavar="name", help="SMTP FROM name")
    cmd.add_argument("-smtpto", metavar="name", help="SMTP TO name")
    cmd.add_argument("-z", metavar="name", help="ZFS dataset name", required=True)
    cmd.add_argument("-p", metavar="path", help="Path to the directory to store backups", required=True)
    cmd.add_argument("-x", metavar="text", help="Backup file name prefix ({})".format(defaults["prefix"]),
                     default=defaults["prefix"])
    cmd.add_argument("-r", metavar="timestr", help="Retention time of old files, w(eeks)d(days)h(ours)m(inutes). "
                                                   "If given, the script performs clean up")
    cmd.add_argument("-m", help="Simulate file removal - just write a log record", action="store_true", default=False)
    cmd.add_argument("-l", metavar="text", help="Distinguishing log, known hosts and paramiko log file name suffix")
    cmdargs = cmd.parse_args()
    extrastream = io.StringIO()
    message = io.StringIO()
    message_subject = "ZFS {} backup: ".format(cmdargs.z)
    log = getLogger(cmdargs.l, extrastream)
    basefile = "{}_{}_{}.gz".format(cmdargs.x, cmdargs.z.replace("/", "_SLASH_"), _NOW)
    file = os.path.join(cmdargs.p, basefile)
    snapshot = "{}@{}".format(cmdargs.z, _NOW)
    known_hosts = os.path.join(os.path.dirname(sys.argv[0]),
                               defaults["known_hosts"].format("_{}".format(cmdargs.l) if cmdargs.l else ""))
    paramiko_log = os.path.join(os.path.dirname(sys.argv[0]),
                               defaults["paramiko_log"].format("_{}".format(cmdargs.l) if cmdargs.l else ""))
    print("The script is to be run as {}".format(os.path.abspath(sys.argv[0])), file=message)
    if cmdargs.sshhost:
        print("The backup is to be sent to the remote host {}".format(cmdargs.sshhost), file=message)
    else:
        print("The backup is to be stored locally", file=message)
    print("The backup path is {}".format(cmdargs.p), file=message)
    print("The backup file is {}".format(basefile), file=message)
    print("Debug log and Paramiko log files can be found in the script's directory "
          "(corresponding files with distinguishing suffix, see -l command line argument)", file=message)
    try:
        if cmdargs.r is not None:
            cmdargs.r = getTimeDelta(cmdargs.r, log)
            log.debug("Retention time is set to {}".format(cmdargs.r))
        sftp = None
        if cmdargs.sshhost:
            paramiko_log_to_file(paramiko_log)
            client = paramiko.SSHClient()
            client.load_system_host_keys(known_hosts)
            client.connect(cmdargs.sshhost, cmdargs.sshport, cmdargs.sshuser, cmdargs.sshpass)
            sftp = client.open_sftp()
        if sftp:
            Zfs(log).snapshot_send(snapshot, file, SFTPOutfile(sftp))
            if cmdargs.r:
                SFTPRemoveOld(sftp, cmdargs.p, basefile, cmdargs.x, cmdargs.r, cmdargs.m, log)
        else:
            Zfs(log).snapshot_send(snapshot, file, RegularOutFile())
            if cmdargs.r:
                RegularRemoveOld(cmdargs.p, basefile, cmdargs.x, cmdargs.r, cmdargs.m, log)
    except Exception:
        log.exception("Exception occurred in the main block")
        message_subject = message_subject + "PROBLEM"
        print("Something went wrong", file=message)
    else:
        message_subject = message_subject + "OK"
        print("Everything is OK", file=message)
    finally:
        print("Below is the log of the operation", file=message)
        print(extrastream.getvalue(), file=message)
    if cmdargs.smtphost:
        msg = EmailMessage()
        msg["Subject"] = message_subject
        msg["From"] = cmdargs.smtpfrom
        msg["To"] = cmdargs.smtpto
        msg["Date"] = email.utils.formatdate()
        msg.set_content(message.getvalue())
        s = smtplib.SMTP(cmdargs.smtphost, cmdargs.smtpport)
        s.send_message(msg)
        s.quit()
