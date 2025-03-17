#!/usr/bin/env python3

import boto3
import logging
from logging import Logger, getLogger
from fnmatch import fnmatch
from botocore.exceptions import ClientError as BotoClientError
from argparse import ArgumentParser
from sys import exit
from json import dumps


class S3ToAmi:
    STATUS_UNKNOWN = 0
    STATUS_COMPLETED = 1
    STATUS_ACTIVE = 2
    STATUS_FAILED = 3

    def __init__(self, bucket, log : Logger, object_filter = "*", aws_profile="default", dry_run=False):
        self.profile = aws_profile
        self.bucket = bucket
        self.log = log
        self.object_filter = object_filter
        self.importer_name = "S3ToAmi"
        self.name_prefix = "AMI"
        self.session =  boto3.Session(profile_name=self.profile)
        self.s3 = self.session.client("s3")
        self.ec2 = self.session.client("ec2")
        self.state = {
            "s3objects": list(),
            "ec2images": list(),
            "tasks": list()
        }
        self.dry_run = dry_run
        self.status = S3ToAmi.STATUS_ACTIVE
        self.get_s3objects()
        self.get_ec2images()
        self.create_tasks()

    @property
    def is_failed(self):
        if self.status == S3ToAmi.STATUS_FAILED:
            return True
        return False

    def get_s3objects(self):
        response = self.s3.list_objects(Bucket=self.bucket)
        for s3object in response["Contents"]:
            if fnmatch(s3object["Key"], self.object_filter):
                self.log.info("Found object {Key}, ETag {ETag}".format(**s3object))
                self.state["s3objects"].append(s3object)
            else:
                self.log.warning("Filtered out object {Key}".format(**s3object))

    def get_ec2images(self):
        response = self.ec2.describe_images(Filters=[
            {"Name": "is-public", "Values": ["false"]},
            {"Name": "tag:Importer", "Values": [self.importer_name]},
            {"Name": "tag:S3Bucket", "Values": [self.bucket]},
            {"Name": "tag-key", "Values": ["S3Key"]},
            {"Name": "tag-key", "Values": ["S3ETag"]}
        ])
        for ec2image in response["Images"]:
            self.log.info("Found image {Name}".format(**ec2image))
            self.state["ec2images"].append(ec2image)

    def create_tasks(self):
        for s3object in self.state["s3objects"]:
            task = {
                "s3object": s3object,   # s3object to import
                "ec2image": None,       # s3image to replace
                "deregister_image": {"status": S3ToAmi.STATUS_UNKNOWN},
                "delete_snapshot": {"status": S3ToAmi.STATUS_UNKNOWN},
                "import_snapshot": {"status": S3ToAmi.STATUS_UNKNOWN, "import_task": None},
                "tag_snapshot": {"status": S3ToAmi.STATUS_UNKNOWN},
                "register_image": {"status": S3ToAmi.STATUS_UNKNOWN}
            }
            found_object = False
            for ec2image in self.state["ec2images"]:
                found_key = False
                found_etag = False
                for tag in ec2image["Tags"]:
                    if tag["Key"] in ("S3Key", "S3ETag") and s3object[tag["Key"][2:]] == tag["Value"]:
                        if tag["Key"] == "S3Key":
                            found_key = True
                        if tag["Key"] == "S3ETag":
                            found_etag = True
                if found_key:
                    if found_etag:
                        self.log.info("Object {Key} already imported".format(**s3object))
                        found_object = True
                    else:
                        task["ec2image"] = ec2image
                    break
            if not found_object:
                if task["ec2image"] is not None:
                    self.log.warning("Add task for object {Key} to replace existing image ({ImageId})".format(
                        Key=s3object["Key"],
                        ImageId=task["ec2image"]["ImageId"]))
                else:
                    self.log.info("Add task for object {Key} to create a new image".format(**s3object))
                self.state["tasks"].append(task)

    def deregister_image(self, task):
        if task["ec2image"] is not None:
            image_id = task["ec2image"]["ImageId"]
            try:
                self.log.warning("Deregister image {}".format(image_id))
                self.ec2.deregister_image(ImageId=image_id)
                task["deregister_image"]["status"] = S3ToAmi.STATUS_COMPLETED
            except BotoClientError as error:
                self.log.error("Could not deregister image {}, {}".format(image_id, error.response["Error"]))
                task["deregister_image"]["status"] = S3ToAmi.STATUS_FAILED
                self.status = S3ToAmi.STATUS_FAILED

    def delete_snapshot(self, task):
        if task["ec2image"] is not None and task["deregister_image"]["status"] == S3ToAmi.STATUS_COMPLETED:
            snapshot_id = None
            for devmap in task["ec2image"]["BlockDeviceMappings"]:
                if devmap["DeviceName"] == "/dev/sda1":
                    snapshot_id = devmap["Ebs"]["SnapshotId"]
                    break
            try:
                self.log.warning("Delete snapshot {}".format(snapshot_id))
                self.ec2.delete_snapshot(SnapshotId=snapshot_id)
                task["delete_snapshot"]["status"] = S3ToAmi.STATUS_COMPLETED
            except BotoClientError as error:
                self.log.error("Could not delete snapshot {}, {}".format(snapshot_id, error.response["Error"]))
                task["delete_snapshot"]["status"] = S3ToAmi.STATUS_FAILED
                self.status = S3ToAmi.STATUS_FAILED

    def import_snapshot(self, task):
        if task["delete_snapshot"]["status"] != S3ToAmi.STATUS_FAILED:
            try:
                self.log.info("Create snapshot import task for {Key}".format(**task["s3object"]))
                response = self.ec2.import_snapshot(
                    Description="{} import".format(self.bucket),
                    DiskContainer={
                        "Description": "{} image".format(self.bucket),
                        "UserBucket": {
                            "S3Bucket": self.bucket,
                            "S3Key": task["s3object"]["Key"]
                        }
                    }
                )
                task["import_snapshot"] = {"status": S3ToAmi.STATUS_ACTIVE, "import_task": response}
                self.log.info("Import task for {} created, Id {}".format(
                    task["s3object"]["Key"], response["ImportTaskId"]))
            except BotoClientError as error:
                self.log.error("Could not create snapshot import task from {}, {}".format(
                    task["s3object"]["Key"], error.response["Error"]))
                task["import_snapshot"]["status"] = S3ToAmi.STATUS_FAILED
                self.status = S3ToAmi.STATUS_FAILED

    def wait_for_import(self):
        imports = list()
        tasks = list()
        for task in self.state["tasks"]:
            if task["import_snapshot"]["status"] == S3ToAmi.STATUS_ACTIVE:
                imports.append(task["import_snapshot"]["import_task"]["ImportTaskId"])
                tasks.append(task)
        if len(imports) > 0:
            self.log.info("Wait for import tasks to complete ({})".format(", ".join(imports)))
            self.ec2.get_waiter("snapshot_imported").wait(ImportTaskIds=imports,
                                                          WaiterConfig={"MaxAttempts": 120, "Delay": 15})
            response = self.ec2.describe_import_snapshot_tasks(ImportTaskIds=imports)
            for task in tasks:
                task["import_snapshot"]["status"] = S3ToAmi.STATUS_COMPLETED
                for completed_task in response["ImportSnapshotTasks"]:
                    if completed_task["ImportTaskId"] == task["import_snapshot"]["import_task"]["ImportTaskId"]:
                        task["import_snapshot"]["import_task"] = completed_task
                        break

    def tag_snapshot(self, task):
        if task["import_snapshot"]["status"] == S3ToAmi.STATUS_COMPLETED:
            snapshot_id = task["import_snapshot"]["import_task"]["SnapshotTaskDetail"]["SnapshotId"]
            try:
                self.log.info("Add tags to snapshot {} ({})".format(snapshot_id,task["s3object"]["Key"]))
                self.ec2.create_tags(Resources=[snapshot_id],
                                     Tags=[
                                         {
                                             "Key": "Importer",
                                             "Value": self.importer_name
                                         },
                                         {
                                             "Key": "Name",
                                             "Value": "-".join((self.name_prefix, task["s3object"]["Key"]))
                                         },
                                         {
                                             "Key": "S3Bucket",
                                             "Value": self.bucket
                                         },
                                         {
                                             "Key": "S3Key",
                                             "Value": task["s3object"]["Key"]
                                         },
                                         {
                                             "Key": "S3ETag",
                                             "Value": task["s3object"]["ETag"]
                                         }
                                     ])
                task["tag_snapshot"]["status"] = S3ToAmi.STATUS_COMPLETED
            except BotoClientError as error:
                self.log.error("Could not add tags to snapshot {}, {}".format(snapshot_id,
                                                                              error.response["Error"]))
                task["tag_snapshot"]["status"] = S3ToAmi.STATUS_FAILED
                self.status = S3ToAmi.STATUS_FAILED

    def register_image(self, task):
        if task["tag_snapshot"]["status"] == S3ToAmi.STATUS_COMPLETED:
            snapshot_id = task["import_snapshot"]["import_task"]["SnapshotTaskDetail"]["SnapshotId"]
            try:
                self.log.info("Register image {} ({})".format(task["s3object"]["Key"], snapshot_id))
                self.ec2.register_image(
                    Name=task["s3object"]["Key"],
                    Description="{} image".format(self.bucket),
                    Architecture="x86_64",
                    RootDeviceName="/dev/sda1",
                    BlockDeviceMappings=[
                        {
                            "DeviceName": "/dev/sda1",
                            "Ebs": {
                                "DeleteOnTermination": True,
                                "SnapshotId": snapshot_id
                            }
                        },
                        {
                            "DeviceName": "/dev/sdb",
                            "VirtualName": "ephemeral0"
                        },
                        {
                            "DeviceName": "/dev/sdc",
                            "VirtualName": "ephemeral1"
                        }
                    ],
                    BootMode="uefi-preferred",
                    VirtualizationType="hvm",
                    TagSpecifications=[
                        {
                            "ResourceType": "image",
                            "Tags": [
                                {
                                    "Key": "Importer",
                                    "Value": self.importer_name
                                },
                                {
                                    "Key": "Name",
                                    "Value": "-".join((self.name_prefix, task["s3object"]["Key"]))
                                },
                                {
                                    "Key": "S3Bucket",
                                    "Value": self.bucket
                                },
                                {
                                    "Key": "S3Key",
                                    "Value": task["s3object"]["Key"]
                                },
                                {
                                    "Key": "S3ETag",
                                    "Value": task["s3object"]["ETag"]
                                }
                            ]
                        }
                    ]
                )
                task["register_image"]["status"] = S3ToAmi.STATUS_COMPLETED
            except BotoClientError as error:
                self.log.error("Could not register image {}, {}".format(snapshot_id,
                                                                              error.response["Error"]))
                task["register_image"]["status"] = S3ToAmi.STATUS_FAILED
                self.status = S3ToAmi.STATUS_FAILED

    def process_tasks(self):
        if not self.dry_run:
            for task in self.state["tasks"]:
                self.deregister_image(task)
                self.delete_snapshot(task)
                self.import_snapshot(task)
            self.wait_for_import()
            for task in self.state["tasks"]:
                self.tag_snapshot(task)
                self.register_image(task)
        else:
            self.log.warning("Tasks are not processed in Dry Run mode")
        if self.status == S3ToAmi.STATUS_ACTIVE:
            self.status = S3ToAmi.STATUS_COMPLETED


if __name__ == "__main__":
    logging.basicConfig(level=logging.NOTSET, handlers=[logging.NullHandler()])
    log_formatter = logging.Formatter(fmt="[{levelname}] {message}", style="{")
    log_handler = logging.StreamHandler()
    log_handler.setFormatter(log_formatter)
    log_handler.setLevel(logging.NOTSET)
    log_logger = getLogger("import_session")
    log_logger.addHandler(log_handler)
    log_logger.setLevel(logging.NOTSET)
    log_logger.propagate = False

    parser = ArgumentParser(description="Import VM images from S3 bucket to EC2")
    parser.add_argument("--bucket", required=True, help="S3 bucket")
    parser.add_argument("--filter", default="*", dest="object_filter", help="Glob filter for bucket keys")
    parser.add_argument("--profile", dest="aws_profile", default="default", help="AWS profile")
    parser.add_argument("--dry-run", dest="dry_run", default=False, action="store_true", help="Dry run")
    args = vars(parser.parse_args())

    session = S3ToAmi(log=log_logger, **args)
    session.process_tasks()
    if session.is_failed:
        exit(1)

    # print(dumps(session.state, indent=2, skipkeys=True, default=str))
