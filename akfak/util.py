import os

from hashlib import sha256
from copy import deepcopy

import yaml


def supplement(default: dict, config: dict, ignore: list = None) -> None:
    """
    Using default config and cluster config, modify config in place with
    defaults. Will perform this recursively as long as the value is a dict.

    Args:
        default (dict): default settings config
        config (dict): target config to modify
        ignore (list): list of keys to ignore while walking
    """
    if not ignore:
        ignore = []

    for name, value in default.items():

        if name in ignore:
            continue

        if name not in config:
            config[name] = value
        elif isinstance(value, dict):
            supplement(value, config[name])


def build_discovery_playlist(config: dict) -> dict:
    """
    Build discovery playlist for Zabbix

    Args:
        config (dict): target config to modify

    Returns:
        formatted dict for zabbix
    """
    payload = {}
    for cluster in config['clusters']:
        clstr_zbx = cluster['settings'].get('zabbix', {})
        name = clstr_zbx.get('name', cluster['name'])
        payload.setdefault(name, {'data': []})
        cluster_payload = payload[name]['data']
        for topic, topic_config in cluster['topics'].items():
            for consumer in topic_config['consumers'].keys():
                cluster_payload.append({
                    '{#TOPIC}': topic,
                    '{#CONSUMER}': consumer
                })

    return payload


def build_config(config: dict) -> None:
    """
    Modify config in place, using default settings to fill in the gaps.

    Args:
        config (dict): raw config dict from :func:`load_config`
    """
    default_settings = config.setdefault('settings', {})
    clusters = config['clusters']

    # set correct type if empty
    if not isinstance(default_settings, dict):
        config.update({'settings': {}})
    default_settings = config['settings']

    # set some defaults, hence the name
    default_settings.setdefault('parallel_fetch', True)
    default_settings.setdefault('server_output', './')
    default_settings.setdefault('server_interval', 10)
    default_settings.setdefault('server_timeout', 10)
    default_settings.setdefault(
        'alerts', {'average': 25000000,
                   'high': 50000000,
                   'disaster': 75000000}
    )

    if 'zabbix' in default_settings:
        zabbix_settings = default_settings.setdefault('zabbix', {})
        zabbix_settings.setdefault('key', 'kafka.lag')
        zabbix_settings.setdefault('port', 10051)

    if 'graphite' in default_settings:
        graphite_settings = default_settings.setdefault('graphite', {})
        graphite_settings.setdefault('port', 2003)

    # setup settings
    for cluster in clusters:

        # setup defaults for cluster settings
        if 'settings' not in cluster:
            cluster['settings'] = deepcopy(default_settings)
            continue

        settings = cluster['settings']
        supplement(default_settings, settings, ['alerts'])

    # setup alerting values
    default_alerts = default_settings['alerts']
    for cluster in clusters:

        # setup cluster alert defaults
        cluster_settings = cluster['settings']
        cluster_settings.setdefault('alerts', default_alerts)
        cluster_alerts = cluster_settings['alerts']

        # set default alert values for each topic:consumer pair
        for tname, topic_config in cluster['topics'].items():
            for cname, consumer_config in topic_config['consumers'].items():
                if not consumer_config:
                    topic_config['consumers'][cname] = {}
                    consumer_config = topic_config['consumers'][cname]
                consumer_config.setdefault('alerts', cluster_alerts)


def read_config(config: str) -> dict:
    """
    Load config from YAML and expand env vars.

    Args:
        config (str): path to config

    Returns:
        config dictionary
    """
    with open(config) as f:
        data = f.read()
    data = os.path.expandvars(data)
    data = yaml.safe_load(data)
    return data


def load_config(config: str) -> dict:
    """
    Load & build the config.

    Args:
        config (str): path to yaml config

    Returns:
        complete config dict
    """
    new_config = read_config(config)
    build_config(new_config)
    return new_config


def sha256sum(path: str) -> str:
    """
    sha256 sum file contents

    Args:
        path (str): path to file

    Returns:
        sha256 sum of file
    """
    fsum = sha256()
    with open(path, 'r', encoding='utf-8') as fi:
        fsum.update(bytes(fi.read(), encoding='utf-8'))
    return fsum.hexdigest()
