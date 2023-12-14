
from __future__ import print_function
from os import environ, unlink
from os.path import basename, exists
from sys import exc_info, argv
from sqlalchemy import create_engine, text
from threading import Thread
from Queue import Queue, Empty
from argparse import ArgumentParser
from random import choice, randint
from time import sleep
from timeit import default_timer
from datetime import datetime
from copy import copy
from math import ceil
from signal import signal, SIGINT
import socket
from asyncore import dispatcher, loop
from asynchat import async_chat
from subprocess import Popen
import pickle
from base64 import b64encode, b64decode


Queries = {
    "q1": {
        "text": text("""
            SELECT
                project_id, COUNT(*)
            FROM
                instances
            WHERE
                project_id = :p_project_id
        """),
        "params": {
            "p_project_id": "a_project"
        },
        "params_map": {
            "p_project_id": "project"
        }
    },
    "q2": {
        "text": text("""
            SELECT
              aggregates.created_at AS aggregates_created_at,
              aggregates.updated_at AS aggregates_updated_at,
              aggregates.deleted_at AS aggregates_deleted_at,
              aggregates.deleted AS aggregates_deleted,
              aggregates.id AS aggregates_id,
              aggregates.uuid AS aggregates_uuid,
              aggregates.name AS aggregates_name,
              aggregate_hosts_1.created_at AS aggregate_hosts_1_created_at,
              aggregate_hosts_1.updated_at AS aggregate_hosts_1_updated_at,
              aggregate_hosts_1.deleted_at AS aggregate_hosts_1_deleted_at,
              aggregate_hosts_1.deleted AS aggregate_hosts_1_deleted,
              aggregate_hosts_1.id AS aggregate_hosts_1_id,
              aggregate_hosts_1.host AS aggregate_hosts_1_host,
              aggregate_hosts_1.aggregate_id AS aggregate_hosts_1_aggregate_id,
              aggregate_metadata_1.created_at AS aggregate_metadata_1_created_at,
              aggregate_metadata_1.updated_at AS aggregate_metadata_1_updated_at,
              aggregate_metadata_1.deleted_at AS aggregate_metadata_1_deleted_at,
              aggregate_metadata_1.deleted AS aggregate_metadata_1_deleted,
              aggregate_metadata_1.id AS aggregate_metadata_1_id,
              aggregate_metadata_1.`key` AS aggregate_metadata_1_key,
              aggregate_metadata_1.value AS aggregate_metadata_1_value,
              aggregate_metadata_1.aggregate_id AS aggregate_metadata_1_aggregate_id
            FROM
              aggregates INNER JOIN aggregate_hosts ON
                aggregates.id = aggregate_hosts.aggregate_id AND
                aggregate_hosts.deleted = :p_deleted_1 AND
                aggregates.deleted = :p_deleted_2 INNER JOIN aggregate_metadata ON
                  aggregates.id = aggregate_metadata.aggregate_id AND
                  aggregate_metadata.deleted = :p_deleted_3 AND
                  aggregates.deleted = :p_deleted_4 LEFT OUTER JOIN aggregate_hosts AS aggregate_hosts_1 ON
                    aggregates.id = aggregate_hosts_1.aggregate_id AND
                    aggregate_hosts_1.deleted = :p_deleted_5 AND
                    aggregates.deleted = :p_deleted_6 LEFT OUTER JOIN aggregate_metadata AS aggregate_metadata_1 ON
                      aggregates.id = aggregate_metadata_1.aggregate_id AND
                      aggregate_metadata_1.deleted = :p_deleted_7 AND
                      aggregates.deleted = :p_deleted_8
            WHERE
              aggregates.deleted = :p_deleted_9 AND
              aggregate_hosts.host = :p_host_1 AND
              aggregate_metadata.key = :p_key_1
        """),
        "params": {
            'p_host_1': 'cmp045',
            'p_deleted_9': 0,
            'p_deleted_8': 0,
            'p_deleted_7': 0,
            'p_deleted_6': 0,
            'p_deleted_5': 0,
            'p_deleted_4': 0,
            'p_deleted_3': 0,
            'p_deleted_2': 0,
            'p_deleted_1': 0,
            'p_key_1': 'availability_zone'
        },
        "params_map": {
            "p_host_1": "host"
        }
    },
    "q3": {
        "text": text("""
            SELECT
              quotas.created_at AS quotas_created_at,
              quotas.updated_at AS quotas_updated_at,
              quotas.deleted_at AS quotas_deleted_at,
              quotas.deleted AS quotas_deleted,
              quotas.id AS quotas_id, quotas.project_id AS quotas_project_id, 
              quotas.resource AS quotas_resource, 
              quotas.hard_limit AS quotas_hard_limit
            FROM
              quotas
            WHERE
              quotas.deleted = :p_deleted_1 AND
              quotas.project_id = :p_project_id_1
        """),
        "params": {
            "p_deleted_1": 0,
            "p_project_id_1": "a_project"
        },
        "params_map": {
            "p_project_id_1": "project"
        }
    }
}


def debug_msg(msg):
    print ("DEBUG: {}".format(msg))


class SigHandler:
    def __init__(self):
        self.sigint_caught = False

    def handler(self, signum, frame):
        if signum == SIGINT:
            self.sigint_caught = True


class QueueItem:
    def __init__(self, i_type, i_payload=None):
        self.type = i_type
        self.payload = i_payload

    def set_type(self, i_type):
        self.type = i_type

    def get_type(self):
        return self.type

    def get_payload(self):
        return self.payload


class Message(QueueItem):
    DONE = 0
    STOPPED = 1
    EXCEPTION = 2

    @property
    def is_done(self):
        return self.type == Message.DONE

    @property
    def is_stopped(self):
        return self.type == Message.STOPPED

    @property
    def is_exception(self):
        return self.type == Message.EXCEPTION


class Task(QueueItem):
    EXECUTE = 0
    STOP = 1

    @property
    def is_execute(self):
        return self.type == Task.EXECUTE

    @property
    def is_stop(self):
        return self.type == Task.STOP


class ThreadPoolQueryExecutor:
    def __init__(self, engine_url, engine_params, thread_pool_size, nitems):
        self.engine = create_engine(engine_url, **engine_params)
        self.tasks = Queue()
        self.tasks_added = 0
        self.tasks_dropped = 0
        self.messages = Queue()
        self.pool = [Thread(target=self.query_executor) for x in range(0, thread_pool_size)]
        self.nitems = nitems
        for t in self.pool:
            t.start()

    def add_task(self, task, force=False):
        if not force and self.tasks.qsize() >= self.num_alive():
            self.tasks_dropped += 1
        else:
            self.tasks.put(task)
            self.tasks_added += 1

    def __del__(self):
        for t in self.pool:
            t.join()

    def num_alive(self):
        i = 0
        for t in self.pool:
            if t.isAlive():
                i += 1
        return i

    def query_executor(self):
        q_projects = text("""
            SELECT
                DISTINCT project_id
            FROM
                instances
            WHERE
                project_id IS NOT NULL
        """)
        q_hosts = text("""
            SELECT
                DISTINCT host
            FROM
                instances
            WHERE
                host IS NOT NULL
        """)
        try:
            with self.engine.connect() as conn1:
                projects, hosts = ([r[0] for r in conn1.execute(q).fetchall()] for q in (q_projects, q_hosts))
                params_subst = {
                    "project": projects,
                    "host": hosts
                }
                self.messages.put(Message(Message.DONE, params_subst))
            do_loop = True
            while do_loop:
                q_num = randint(self.nitems, self.nitems * 10)
                with self.engine.connect() as conn2:
                    # Simulate session of n-queries after which the connection will be closed
                    for q_counter in range(0, q_num):
                        task = self.tasks.get()
                        if task.is_stop:
                            self.messages.put(Message(Message.STOPPED))
                            do_loop = False
                            break
                        elif task.is_execute:
                            q = task.get_payload()
                            for k, v in q["params_map"].items():
                                if v in params_subst:
                                    q["params"][k] = choice(params_subst[v])
                            self.messages.put(Message(Message.DONE, conn2.execute(q["text"], **q["params"]).fetchall()))
        except:
            self.messages.put(Message(Message.EXCEPTION, exc_info()[0:2]))


class MessageCounter:
    def __init__(self):
        self.done_count = 0
        self.stopped_count = 0
        self.exception_count = 0
        self.exception_collect = list()

    def add_message(self, message):
        if message.is_done:
            self.done_count += 1
        elif message.is_stopped:
            self.stopped_count += 1
        elif message.is_exception:
            self.exception_count += 1
            self.exception_collect.append(message.get_payload())

    def get_snap(self):
        snap = MessageCounter()
        snap.done_count = self.done_count
        snap.stopped_count = self.stopped_count
        snap.exception_count = self.exception_count
        snap.exception_collect = copy(self.exception_collect)
        return snap

    def __sub__(self, other):
        diff = MessageCounter()
        diff.done_count = self.done_count - other.done_count
        diff.stopped_count = self.stopped_count - other.stopped_count
        diff.exception_count = self.exception_count - other.exception_count
        if len(self.exception_collect) > len(other.exception_collect):
            diff.exception_collect = self.exception_collect[len(other.exception_collect):]
        return diff


class RoundAdjuster:
    def __init__(self, nitems, nrounds=10, check_interval=1):
        self.timings = list()
        self.checkpoint = list()
        self.nitems = nitems
        self.nrounds = nrounds
        self.check_interval = check_interval

    def checkpoint_start(self):
        self.checkpoint = list()
        self.checkpoint.append(default_timer())

    def checkpoint_end(self):
        self.checkpoint.append(default_timer())
        self.timings.append(self.checkpoint)
        if len(self.timings) > 2:
            del self.timings[0]

    def get_sleep_time(self):
        desired_sleep_time = float(self.check_interval) / self.nrounds
        round_time_spent = self.timings[0][1] - self.timings[0][0] if len(self.timings) == 2 else 0
        return desired_sleep_time - round_time_spent if desired_sleep_time > round_time_spent else 0

    def get_nitems_per_round(self):
        return int(max(ceil(self.nitems / self.nrounds), 1))

    def get_nrounds(self):
        return self.nrounds


class Data:
    DATA = 0
    EXCEPTION = 1
    TERMINATOR = "|"

    def __init__(self, _data_type = 0, **kwargs):
        self.type = _data_type
        self.payload = kwargs

    def get_type(self):
        return self.type

    def get_payload(self):
        return self.payload

    def loads(self, data_str):
        loaded = pickle.loads(b64decode(data_str))
        self.type = loaded.type
        self.payload = loaded.payload
        return self

    def dumps(self):
        return "{}{}".format(b64encode(pickle.dumps(self)), Data.TERMINATOR)

    @property
    def is_data(self):
        return self.type == Data.DATA

    @property
    def is_exception(self):
        return self.type == Data.EXCEPTION


class ClientProducer:
    def __init__(self, task, ThreadPoolQueryExecutor_params, RoundAdjuster_params):
        self.task = task
        self.executor = ThreadPoolQueryExecutor(**ThreadPoolQueryExecutor_params)
        self.counter = MessageCounter()
        self.time_0 = default_timer()
        self.snap_counter = MessageCounter()
        self.adjuster = RoundAdjuster(**RoundAdjuster_params)
        self.tasks_added_0 = 0
        self.tasks_dropped_0 = 0
        self.task_rounds = 0
        self.tasks_per_round = 0
        self.stop_sent = False

    def produce(self):
        try:
            while True:
                self.adjuster.checkpoint_start()
                try:
                    while True:
                        self.counter.add_message(self.executor.messages.get_nowait())
                except Empty:
                    pass
                if self.counter.exception_count > 0:
                    rv = Data(Data.EXCEPTION,
                              source="sql",
                              info=str(self.counter.exception_collect[-1]))
                    self.task.set_type(Task.STOP)
                    break
                else:
                    time_1 = default_timer()
                    time_diff = time_1 - self.time_0
                    if self.task_rounds == self.adjuster.get_nrounds():
                        diff_counter = self.counter - self.snap_counter
                        rv = Data(Data.DATA,
                                  added=self.executor.tasks_added - self.tasks_added_0,
                                  done=diff_counter.done_count,
                                  dropped=self.executor.tasks_dropped - self.tasks_dropped_0,
                                  qsize=self.executor.tasks.qsize(),
                                  alive=self.executor.num_alive(),
                                  psize=self.executor.engine.pool.size(),
                                  checkedout=self.executor.engine.pool.checkedout(),
                                  timediff=time_diff,
                                  rounds=self.task_rounds,
                                  perround=self.tasks_per_round)
                        self.snap_counter = self.counter.get_snap()
                        self.time_0 = time_1
                        self.tasks_added_0 = self.executor.tasks_added
                        self.tasks_dropped_0 = self.executor.tasks_dropped
                        self.task_rounds = 0
                        break
                    else:
                        self.tasks_per_round = 0
                        self.task_rounds += 1
                        for i in range(0, self.adjuster.get_nitems_per_round()):
                            self.tasks_per_round += 1
                            self.executor.add_task(Task(Task.EXECUTE, choice(Queries.values())))
                        self.adjuster.checkpoint_end()
                        sleep(self.adjuster.get_sleep_time())
        except Exception:
            rv = Data(Data.EXCEPTION,
                      source="main",
                      info=str(exc_info()[0:2]))
            self.task.set_type(Task.STOP)
        return rv

    def more(self):
        if self.task.is_execute:
            return self.produce().dumps()
        elif self.task.is_stop:
            if not self.stop_sent:
                for i in range(0, self.executor.num_alive()):
                    self.executor.add_task(Task(Task.STOP), force=True)
                self.stop_sent = True
            sleep(1)
            if self.executor.num_alive() > 0:
                return Data(Data.DATA,
                            qsize=self.executor.tasks.qsize(),
                            alive=self.executor.num_alive(),
                            psize=self.executor.engine.pool.size(),
                            checkedout=self.executor.engine.pool.checkedout()
                            ).dumps()
            else:
                return ""


class Client(async_chat):
    def __init__(self, sock_path, ThreadPoolQueryExecutor_params, RoundAdjuster_params):
        async_chat.__init__(self)
        self.data = list()
        self.task = Task(Task.EXECUTE)
        self.set_terminator(Data.TERMINATOR)
        self.create_socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.connect(sock_path)
        self.push_with_producer(ClientProducer(self.task, ThreadPoolQueryExecutor_params, RoundAdjuster_params))
        self.close_when_done()

    def collect_incoming_data(self, data):
        self.data.append(data)

    def found_terminator(self):
        self.task.set_type(Data().loads("".join(self.data)).get_payload()["task"].get_type())
        self.data = list()


class DataCollector:
    D_EXCEPTION = 0
    D_EXECUTION = 1
    D_FINALIZATION = 3
    def __init__(self):
        self.data = {DataCollector.D_EXECUTION: list(), DataCollector.D_EXCEPTION: list(),
                     DataCollector.D_FINALIZATION: list()}
        self.time_0 = default_timer()
        self.start_time = datetime.utcnow().replace(microsecond=0)

    def print_data(self, data_type):
        elapsed = str(datetime.utcnow().replace(microsecond=0) - self.start_time)
        if data_type in (DataCollector.D_EXECUTION, DataCollector.D_FINALIZATION):
            data_keys = {
                DataCollector.D_EXECUTION: ("elapsed", "added", "done", "dropped", "qsize", "alive", "psize",
                                            "checkedout", "timediff", "rounds", "perround"),
                DataCollector.D_FINALIZATION: ("elapsed", "qsize", "alive", "psize", "checkedout")
            }
            if len(self.data[data_type]) > 0:
                to_print = self.data[data_type][0].get_payload()
                for item in self.data[data_type][1:]:
                    for k, v in item.get_payload().items():
                        to_print[k] += v
                to_print["elapsed"] = elapsed
                print(", ".join("{}: {}".format(x, to_print[x]) for x in data_keys[data_type]))
        elif data_type == DataCollector.D_EXCEPTION:
            for item in self.data[data_type]:
                print("Exception in {}: {}".format(item.get_payload()["source"], item.get_payload()["info"]))
        self.data[data_type] = list()

    def add(self, data):
        if data.is_exception:
            debug_msg("Add exception")
            self.data[DataCollector.D_EXCEPTION].append(data)
        elif "added" in data.get_payload():
            self.data[DataCollector.D_EXECUTION].append(data)
        else:
            self.data[DataCollector.D_FINALIZATION].append(data)
        time_1 = default_timer()
        if time_1 - self.time_0 >= 1.0:
            self.time_0 = time_1
            self.print_data(DataCollector.D_EXECUTION)
            self.print_data(DataCollector.D_EXCEPTION)
            self.print_data(DataCollector.D_FINALIZATION)


class IncomingClient(async_chat):
    def __init__(self, sock, data_collector, server_state, sighandler, client_name):
        async_chat.__init__(self, sock)
        self.client_name = client_name
        self.raw_data = list()
        self.data_collector = data_collector
        self.set_terminator(Data.TERMINATOR)
        self.server_state = server_state
        self.server_state["accepted"] += 1
        self.sighandler = sighandler
        self.stop_sent = False

    def collect_incoming_data(self, raw_data):
        self.raw_data.append(raw_data)

    def found_terminator(self):
        data = Data().loads("".join(self.raw_data))
        if data.is_exception:
            self.server_state["exception"] = True
            debug_msg("Incoming {} got exception".format(self.client_name))
        elif self.server_state["exception"] and not self.stop_sent:
            self.push(Data(task=Task(Task.STOP)).dumps())
            self.stop_sent = True
            debug_msg("Incoming {} send stop on server state exception".format(self.client_name))
        if self.sighandler.sigint_caught and not self.stop_sent:
            self.push(Data(task=Task(Task.STOP)).dumps())
            self.stop_sent = True
            debug_msg("Incoming {} send stop on signal".format(self.client_name))
        self.data_collector.add(data)
        self.raw_data = list()

    def handle_close(self):
        debug_msg("Close {}".format(self.client_name))
        self.data_collector.print_data(DataCollector.D_EXCEPTION)
        self.data_collector.print_data(DataCollector.D_FINALIZATION)
        self.close()
        self.server_state["closed"] += 1
        if self.server_state["accepted"] == self.server_state["closed"]:
            debug_msg("Close server")
            self.server_state["server_obj"].close()


class Server(dispatcher):
    def __init__(self, sock_path, sighandler):
        dispatcher.__init__(self)
        self.sock_path = sock_path
        self.create_socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.bind(self.sock_path)
        self.listen(10)
        self.incoming_count = 0
        self.server_state = {
            "server_obj": self,
            "accepted": 0,
            "closed": 0,
            "exception": False
        }
        self.sighandler = sighandler
        self.data_collector = DataCollector()

    def handle_accept(self):
        IncomingClient(self.accept()[0], self.data_collector, self.server_state, self.sighandler,
                       "client-{}".format(self.incoming_count))
        debug_msg("Accepted {}".format("client-{}".format(self.incoming_count)))
        self.incoming_count += 1

    def close(self):
        unlink(self.sock_path)
        dispatcher.close(self)


if __name__ == "__main__":
    engine_url = environ["ENGINE_URL"]
    engine_params = {
        "pool_size": int(environ["ENGINE_POOL_SIZE"]),
        "max_overflow": int(environ["ENGINE_MAX_OVERFLOW"]),
        "pool_recycle": int(environ["ENGINE_POOL_RECYCLE"])
    }
    args = ArgumentParser()
    args.add_argument("-n", dest="n", type=int, default=10, help="n queries per sec per process")
    args.add_argument("-p", dest="p", type=int, default=1, help="p processes in parallel")
    args.add_argument("-s", dest="s", type=str, default="",
                      help="server's socket path, indicates the client mode")
    cmdopts = args.parse_args()
    sighandler = SigHandler()
    if cmdopts.s:
        Client(cmdopts.s,
               {
                   "engine_url": engine_url,
                   "engine_params": engine_params,
                   "thread_pool_size": engine_params["pool_size"],
                   "nitems": cmdopts.n},
               {
                   "nitems": cmdopts.n
               })
    else:
        sock_path = "{}.sock".format(basename(argv[0]))
        if exists(sock_path):
            unlink(sock_path)
        Server(sock_path, sighandler)
        for i in range(0, cmdopts.p):
            Popen(("python", argv[0], "-s", sock_path, "-n", str(cmdopts.n)))
    oldsighandler = signal(SIGINT, sighandler.handler)
    loop()
    signal(SIGINT, oldsighandler)
