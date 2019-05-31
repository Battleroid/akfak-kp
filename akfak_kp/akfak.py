import json
import logging
import os
import socket
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, wait
from contextlib import contextmanager
from copy import copy, deepcopy
from queue import Full, Queue
from threading import Thread
from typing import Any, AnyStr, Generator
from uuid import uuid4

import click
import structlog
from kafka import KafkaConsumer, TopicPartition
from pyzabbix import ZabbixMetric, ZabbixSender

from akfak_kp.enums import Level
from akfak_kp.log import setup_logging
from akfak_kp.util import build_discovery_playlist, load_config, sha256sum


# our own logger (for JSON output), null out the pykafka logs
setup_logging()
kafka_log = logging.getLogger('kafka')
kafka_log.setLevel(100)


# TODO: create class for logging using partials to set error/warn/info
@contextmanager
def timeout(t=5.0) -> Generator[Any, Any, None]:
    org_timeout = socket.getdefaulttimeout()
    socket.setdefaulttimeout(t)
    yield
    socket.setdefaulttimeout(org_timeout)


class AkfakClient:
    """
    Manages the fetching & sending of offset info for a single client (cluster)
    """

    def __init__(self, config: dict) -> None:

        # config & settings
        self.uuid = str(uuid4())
        self.name = config['name']
        self.config = config

        # TODO: replace with log.bind and personal loggers for each
        self.log = structlog.get_logger('akfak.client', client=self)
        self.log.info('started_client_setup')

        # setup consumers for every pair
        # TODO: consumer creation should be a func to avoid indent vomit
        sasl_config = self.config['settings'].get('sasl', {})
        sasl_enabled = sasl_config.get('enabled', False)
        if sasl_enabled:
            self.log.info('sasl_settings_found')
        self.kafka_consumers = {}
        for topic, topic_config in self.config['topics'].items():
            self.log.info('creating_consumers_for_topic', topic=topic)
            self.kafka_consumers.setdefault(topic, {})
            for group in topic_config['consumers']:
                try:
                    if sasl_enabled:
                        self.kafka_consumers[topic][group] = KafkaConsumer(
                                group_id=group,
                                bootstrap_servers=self.config['brokers'].split(','),
                                enable_auto_commit=False,
                                sasl_mechanism='PLAIN',
                                sasl_plain_username=sasl_config['username'],
                                sasl_plain_password=sasl_config['password'],
                                security_protocol='SASL_PLAINTEXT',
                                client_id=f'{self.name}-{self.uuid}-{topic}-'
                                          f'{group}'
                        )
                    else:
                        self.kafka_consumers[topic][group] = KafkaConsumer(
                                group_id=group,
                                bootstrap_servers=self.config['brokers'].split(','),
                                enable_auto_commit=False,
                                client_id=f'{self.name}-{self.uuid}-{topic}-'
                                          f'{group}'
                        )

                    # Do metadata update via .topics() because kafka-python for some reason
                    # doesn't automatically fetch the fucking metadata on init
                    self.kafka_consumers[topic][group].topics()
                    if not self.kafka_consumers[topic][group].partitions_for_topic(topic):
                        del self.kafka_consumers[topic][group]
                        raise Exception('Could not get partitions')
                except Exception as e:
                    self.log.error(
                            'kafka_broker_setup_error',
                            exc_info=e,
                            topic=topic,
                            consumer=group
                    )

        # parallel?
        self.fetch_method = self.parallel_fetch_all
        if not self.config['settings']['parallel_fetch']:
            self.fetch_method = self.fetch_all

        # Build list of notifiers for sending
        self.notifiers = []
        for item in ('graphite', 'zabbix'):
            if item in self.config['settings']:
                self.notifiers.append(getattr(self, f'send_to_{item}'))

    def send_to_graphite(self, data: dict) -> None:
        """
        Send results to graphite

        Args:
            data (dict): results dict
        """
        fixed_data = data[next(iter(data))]
        prefix = self.config['settings']['graphite']['prefix']

        if 'error' in fixed_data or not fixed_data or not data:
            return

        payload = []
        for topic, consumers in fixed_data.items():
            for consumer, lag_data in consumers.items():
                epoch = lag_data['epoch']
                total = lag_data['total']
                payload.append(
                        f'{prefix}.{self.name}.{topic}.{consumer}.total '
                        f'{total} {epoch}'
                )
                for part, part_lag_data in lag_data['parts'].items():
                    part_lag = part_lag_data['lag']
                    payload.append(
                            f'{prefix}.{self.name}.{topic}.{consumer}.{part} '
                            f'{part_lag} {epoch}'
                    )

        graphite_url = self.config['settings']['graphite']['url']
        graphite_port = int(self.config['settings']['graphite']['port'])
        chunk_size = 50
        chunks = [
            payload[x:x + chunk_size]
            for x in range(0, len(payload), chunk_size)
        ]
        for i, chunk in enumerate(chunks, 1):
            message = '\n'.join(chunk)
            message += '\n'
            try:
                s = socket.socket()
                s.settimeout(5.0)
                s.connect((graphite_url, graphite_port))
                s.sendall(message.encode('utf-8'))
            except Exception as e:
                self.log.error(
                        'graphite_send_error',
                        chunk=i,
                        total_chunks=len(chunks),
                        payload=chunk,
                        exc_info=e
                )
            finally:
                s.shutdown(socket.SHUT_RDWR)
                s.close()

    def send_to_zabbix(self, data: dict) -> None:
        """
        Send results (evaluated with rules) to zabbix

        Args:
            data (dict): results dict
        """
        zabbix_name = self.config['settings']['zabbix'].get('name', self.name)
        zabbix_url = self.config['settings']['zabbix']['url']
        zabbix_port = int(self.config['settings']['zabbix']['port'])
        zabbix_key = self.config['settings']['zabbix']['key']
        fixed_data = data[next(iter(data))]

        if 'error' in fixed_data or not fixed_data or not data:
            return

        payload = []
        for topic, consumers in fixed_data.items():
            for consumer, lag_data in consumers.items():
                level = lag_data['zabbix']['level']
                epoch = lag_data['epoch']
                metric = ZabbixMetric(
                        zabbix_name,
                        f'{zabbix_key}[{topic},{consumer}]',
                        level.value,
                        clock=epoch
                )
                payload.append(metric)

        try:
            with timeout():
                zabbix_sender = ZabbixSender(zabbix_url, zabbix_port)
                zabbix_sender.send(payload)
        except Exception as e:
            self.log.error('zabbix_send_error', payload=payload, exc_info=e)

    def send_to_notifiers(self, data: dict) -> None:
        """
        Send to applicable notifiers

        Args:
            data (dict): results dict
        """
        for sender in self.notifiers:
            sender(data)

    def fetch(self,
              topic: AnyStr,
              group: AnyStr,
              consumer_config: dict) -> [str, str, dict]:
        """
        Fetch individual topic:consumer pair offset information

        Args:
            topic (AnyStr): topic name
            group (AnyStr): consumer group
            consumer_config (dict): config for topic:consumer pair

        Returns:
            Topic & Group strings, dict of offset information for partitions,
            total, and evaluation of Zabbix rules inserted at the top level
        """

        # consumer
        cons = self.kafka_consumers[topic][group]

        # prep topic information
        try:
            partitions = [
                TopicPartition(topic, p) for p in
                cons.partitions_for_topic(topic)
            ]
        except Exception as e:
            self.log.error(
                    'partitions_setup_error',
                    topic=topic,
                    consumer=group,
                    partitions_for_topic=cons.partitions_for_topic(topic),
                    exc_info=e
            )
            return topic, group, {'error': 'partitions_setup_error'}

        # fetch lag
        results = {'parts': {}}
        start_time = time.time()
        latest_offsets = cons.end_offsets(partitions)
        for partition, latest in latest_offsets.items():
            # if the partition has no committed offsets the current lag will
            # be None
            # replace this with -1 where appropriate
            current = cons.committed(partition) or -1
            lag = 0 if current == -1 else max(latest - current, 0)
            results['parts'][partition.partition] = {
                'lag': lag,
                'latest': latest,
                'current': current,
            }
        fetch_duration = time.time() - start_time

        # some minor stats
        results['total'] = sum([v['lag'] for v in results['parts'].values()])
        results['total_fetch_duration'] = fetch_duration
        results['latest'] = sorted([
            v['latest'] for v in results['parts'].values()
        ])[-1]
        results['current'] = sorted([
            v['current'] for v in results['parts'].values()
        ])[-1]
        results['epoch'] = int(time.time())

        # evaluate lag (for zabbix & human readable api if it exists)
        if self.config['settings'].get('zabbix', None):
            alerts = [(Level[name], value)
                      for name, value in consumer_config['alerts'].items()]
            evaluations = [(results['total'] >= threshold, level)
                           for level, threshold in alerts]
            max_level = list(
                    filter(
                            lambda x: x[0],
                            sorted(evaluations, key=lambda x: x[1],
                                   reverse=True)
                    )
            )
            if not max_level:
                max_level = Level.normal
            else:
                max_level = max_level[0][1]
            results['zabbix'] = {'level': max_level, 'name': max_level.name}

        return topic, group, results

    def fetch_all(self) -> dict:
        """
        Fetch all given topic:consumer pair offset information in serial

        Returns:
            returns complete result dict of all topic:consumer pairs
        """

        results = defaultdict(lambda: defaultdict(dict))
        cluster_name = self.config['name']

        for tname, topic_config in self.config['topics'].items():
            for cname, consumer_config in topic_config['consumers'].items():
                results[cluster_name][tname][cname] = self.fetch(
                        tname,
                        cname,
                        consumer_config,
                )[-1]

        return {'uuid': self.uuid, 'results': results}

    def parallel_fetch_all(self) -> dict:
        """
        Fetch all given topic:consumer pair offset information in parallel
        using a ThreadPoolExecutor, same process used in the AkfakServer loop,
        this is only possible as dpkp/kafka-python are individual topic:consumer
        clients, pykafka clients (though it could be fit the same way) are
        setup as a single client per cluster

        Returns:
            returns complete result dict of all topic:consumer pairs
        """

        results = defaultdict(lambda: defaultdict(dict))
        tasks = []
        cluster_name = self.config['name']

        tpe = ThreadPoolExecutor(
                thread_name_prefix=f'{self}.parallel_fetch_all')

        for tname, topic_config in self.config['topics'].items():
            for cname, consumer_config in topic_config['consumers'].items():
                tasks.append(
                        tpe.submit(self.fetch, tname, cname, consumer_config))

        done, pending = wait(tasks,
                             timeout=self.config['settings']['server_timeout'])

        tpe.shutdown(False)

        if pending:
            self.log.warn('client_fetch_tasks_exceeded_timeout',
                          exceeded_tasks=pending)

        for task in pending:
            task.cancel()

        for task in done:
            tname, cname, result = task.result()
            results[cluster_name][tname][cname] = result

        return {'uuid': self.uuid, 'results': results}

    def stop(self) -> None:
        for topic, consumers in self.kafka_consumers.items():
            for consumer, client in consumers.items():
                client.close(False)
        self.log.info('stopped_consumers')

    def __del__(self) -> None:
        self.stop()

    def __repr__(self) -> str:
        return f'<AkfakClient {self.uuid}:{self.name}>'


class AkfakServer:
    """
    Manages several Akfak clients from the given config dict
    """

    def __init__(self, config: str) -> None:

        # logging
        self.log = structlog.get_logger('akfak.server', name=self)
        self.log.info('started_setup')

        # config dict
        startup_time = time.time()
        self.path = config
        self.config = load_config(config)
        self.org_sha = sha256sum(self.path)

        self.server_output = os.path.join(
                self.config['settings']['server_output'], 'server.json'
        )
        self.server_discovery = os.path.join(
                self.config['settings']['server_output'], 'discovery.json'
        )
        self.server_interval = self.config['settings']['server_interval']
        self.server_timeout = self.config['settings']['server_timeout']

        # clients
        self.clients = {}
        try:
            for cluster_config in self.config['clusters']:
                client = AkfakClient(cluster_config)
                self.clients[client.uuid] = client
        except Exception as e:
            self.log.error('broker_setup_failure', exc_info=e)
            self.stop()
            raise click.Abort()

        # sending queue & thread
        self.send_queue = Queue(100)
        self.watch_thread = Thread(target=self._watch)
        self.watch_thread.daemon = True

        # signal for stopping
        self.done = False

        self.log.info('finished_setup', duration=time.time() - startup_time,
                      config_sha=self.org_sha)

    def _reload_config(self, new_sha) -> None:
        """
        Reload config with new values, do not apply anything if an exception
        occurs while loading the new clients.
        """
        self.log.info('reload_config_started')
        start_time = time.time()

        # make a deepcopy so we don't just reassign a reference
        _org_config = deepcopy(self.config)
        _org_server_interval = deepcopy(self.server_interval)
        _org_server_timeout = deepcopy(self.server_timeout)
        _org_clients = copy(self.clients)
        _org_sha = copy(self.org_sha)

        try:
            new_config = load_config(self.path)
            server_interval = new_config['settings']['server_interval']
            server_timeout = new_config['settings']['server_timeout']

            # clients
            clients = {}
            for cluster_config in new_config['clusters']:
                client = AkfakClient(cluster_config)
                clients[client.uuid] = client

            # we need the clients to finish sending
            self.log.info(
                    'joining_task_queue',
                    queue_size=self.send_queue.qsize()
            )
            self.send_queue.join()

            # reassign config info
            self.config = new_config
            self.server_interval = server_interval
            self.server_timeout = server_timeout
            self.clients = clients
            self.org_sha = new_sha

            # build discovery playlist
            self.save_discovery_playlist()

        except Exception as e:
            self.log.warn('reload_config_error', exc_info=e)

            # roll it back
            self.config = _org_config
            self.server_interval = _org_server_interval
            self.server_timeout = _org_server_timeout
            self.clients = _org_clients
            self.org_sha = _org_sha
        else:
            self.log.info('reload_config_successful')
        finally:
            self.log.info('reload_config_finished',
                          duration=time.time() - start_time)

    def _watch(self) -> None:
        """
        Target watch process for send thread. Watches and blocks for incoming
        tasks in queue. Uses client :func:`AkfakClient.send_to_notifiers` func
        to send to all relevant notifiers per client.
        """
        # TODO: sending should be per client, so individual clients can send
        # TODO: look into using a TPE to send as a queue is filled (doable)
        # immediately
        while not self.done:
            results, epoch = self.send_queue.get(block=True)
            start_time = time.time()
            for uuid, result in results.items():
                if not result:
                    continue
                client = self.clients[uuid]
                try:
                    client.send_to_notifiers(result)
                except Exception as e:
                    self.log.warn('sending_error',
                                  exc_info=e,
                                  client=client,
                                  payload=result)
            self.send_queue.task_done()
            self.log.info(
                    'sending_finished',
                    task_epoch=epoch,
                    duration=time.time() - start_time
            )

    def save_discovery_playlist(self) -> None:
        """
        Build Zabbix auto discovery playlist and dump to given path
        """

        if 'zabbix' in self.config['settings']:
            with open(self.server_discovery, 'w') as fo:
                json.dump(
                        build_discovery_playlist(self.config),
                        fo,
                        indent=2,
                        sort_keys=True
                )
            self.log.info(
                    'built_discovery_playlist',
                    disc_output=self.server_discovery
            )

    def start(self) -> None:
        """
        Start fetching, notifying and updating
        """

        # build discovery playlist before we start
        self.save_discovery_playlist()

        # Start watching send queue
        self.watch_thread.start()

        while not self.done:

            cur_sha = sha256sum(self.path)
            if cur_sha != self.org_sha:
                self._reload_config(cur_sha)

            if not self.watch_thread.is_alive():
                self.log.warn('watch_thread_died')
                del self.watch_thread
                self.watch_thread = Thread(target=self._watch)
                self.watch_thread.daemon = True
                self.watch_thread.start()

            pe = ThreadPoolExecutor()

            # send = for zabbix/graphite, api = for web (uuid not needed)
            api_results = defaultdict(lambda: defaultdict(dict))
            send_results = {}
            start_time = time.time()

            # with pe as exc:
            tasks = [
                pe.submit(client.fetch_method) for client in
                self.clients.values()
            ]

            done, pending = wait(tasks, timeout=self.server_timeout)

            # force shutdown so the remaining tasks no longer block
            # but take note, the remaining tasks will try to finish
            pe.shutdown(wait=False)

            if pending:
                self.log.warn('tasks_exceeded_timeout', exceeded_tasks=pending)

            for task in pending:
                task.cancel()

            for task in done:
                client_result = task.result()
                uuid = client_result['uuid']
                payload = client_result['results']

                # update results per client
                for client_name, client_payload in payload.items():
                    api_results[client_name].update(client_payload)
                send_results[uuid] = payload

            with open(self.server_output, 'w') as fo:
                fo.write(json.dumps(api_results, indent=2))

            epoch = time.time()
            try:
                self.send_queue.put_nowait((send_results, int(epoch)))
            except Full as e:
                self.log.warn('send_queue_full', results=send_results,
                              epoch=int(epoch), exc_info=e)

            self.log.info(
                    'offset_fetch_finished',
                    task_epoch=int(epoch),
                    duration=time.time() - start_time
            )
            time.sleep(self.server_interval)

    def stop(self) -> None:
        """
        Stop fetching & sending processes
        """
        self.done = True
        self.send_queue.join()
        if self.watch_thread.is_alive():
            self.log.info('shutting_off_send_thread')
            self.watch_thread.join(timeout=5)
        for client in self.clients.values():
            client.stop()
        self.log.info('shutdown_finished')

    def __del__(self) -> None:
        self.log.info('received_shutdown_signal')
        self.stop()

    def __repr__(self) -> str:
        if hasattr(self, 'clients'):
            clients = ", ".join([
                f'{name}:{c.name}' for name, c in self.clients.items()
            ])
        else:
            clients = ''
        return f'<AkfakServer ({clients})>'
