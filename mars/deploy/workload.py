#! /usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import time

from pssh.clients import ParallelSSHClient
import pssh.utils
pssh.utils.enable_host_logger()

try:
    from vineyard.workload import Workload, register_vineyard_workload
except ImportError:
    logging.warning('Cannot be a vineyard workload')

class MarsWorkload(Workload):
    def __init__(self, hosts, client):
        super(MarsWorkload, self).__init__(hosts, client)

        self.scheduler_output = None
        self.worker_outputs = None

        self.create_session()

    @property
    def session(self):
        return self._session

    def create_session(self):
        from ..session import new_session

        scheduler_host, worker_hosts = self.start_mars_cluster()
        self._session = new_session(scheduler_host + ':4000').as_default()

    def start_mars_cluster(self):
        scheduler_host = self._hosts[0]
        worker_hosts = self._hosts
        scheduler_client = ParallelSSHClient([scheduler_host])
        worker_clients = [ParallelSSHClient([worker_host])
                        for worker_host in worker_hosts]

        self.scheduler_output = scheduler_client.run_command(
            'source ~/.zshrc; python3 -m mars.scheduler.__main__ -a %s -p 4000 '
            '--log-level=debug '
            '-Dvineyard.socket=%s > /tmp/mars-scheduler.log 2>&1' % (scheduler_host, self._vineyard_ipc_socket),
            use_pty=True,
        )
        # ensure the scheduler has been launched before workers
        while not _is_port_in_use(scheduler_host, 4000):
            time.sleep(10)
        self.worker_outputs = dict()
        for host, client in zip(worker_hosts, worker_clients):
            print(host, client)
            output = client.run_command(
                'source ~/.zshrc; python3 -m mars.worker.__main__ -a %s -s %s:4000 '
                '--cache-mem=2g --spill-dir=/tmp/ '
                '--log-level=debug '
                '-Dvineyard.socket=%s > /tmp/mars-worker.log 2>&1' % (host, scheduler_host, self._vineyard_ipc_socket),
                use_pty=True,
            )
            self.worker_outputs.update(output)
        time.sleep(10)
        return scheduler_host, worker_hosts

    def apply(self, value):
        return self._session.run(value)


register_vineyard_workload('mars', MarsWorkload)


def _is_port_in_use(host, port):
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex((host, port)) == 0
