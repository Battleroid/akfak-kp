import json
import os
import time

import structlog
from flask import Blueprint, Flask, abort, current_app, jsonify, request
from gevent.wsgi import WSGIServer

from .log import setup_logging

setup_logging()
api_log = structlog.get_logger('akfak.api')
lag_log = structlog.get_logger('akfak.api.lag')
zbx_log = structlog.get_logger('akfak.api.zabbix')

# general lag junk
lag_api = Blueprint('api', __name__, url_prefix='/lag')

@lag_api.before_request
def refresh_lag_data():
    server_output = os.path.join(current_app.g['disc'], 'server.json')
    server_mtime = current_app.g['lag_update']
    current_mtime = os.path.getmtime(server_output)
    if not server_mtime or current_mtime > server_mtime:
        with open(server_output) as fi:
            current_app.g['lag'] = json.load(fi)
        current_app.g['lag_update'] = current_mtime
        lag_log.info('refreshed_lag_data', epoch=time.time(), mtime=current_mtime)


@lag_api.route('/', defaults={'path': None})
@lag_api.route('/<path:path>', strict_slashes=False)
def lag_path(path):
    sub = current_app.g['lag']
    if path:
        split_path = path.split('/')[::-1]
        try:
            while split_path:
                key = split_path.pop()
                sub = sub[key]
        except KeyError as e:
            lag_log.error('invalid_lag_path', path=request.full_path)
            abort(404)
    return jsonify(sub)


# zabbix auto discovery
zabbix_api = Blueprint('zabbix-api', __name__, url_prefix='/zabbix')


@zabbix_api.before_app_first_request
def setup_zabbix_discovery():
    server_output = os.path.join(current_app.g['disc'], 'discovery.json')
    with open(server_output) as fi:
        current_app.g['zabbix'] = json.load(fi)
    zbx_log.info('loaded_discovery_zabbix')


@zabbix_api.before_request
def refresh_zabbix_discovery():
    server_output = os.path.join(current_app.g['disc'], 'discovery.json')
    server_mtime = current_app.g['zabbix_update']
    current_mtime = os.path.getmtime(server_output)
    if not server_mtime or current_mtime > server_mtime:
        with open(server_output) as fi:
            current_app.g['zabbix'] = json.load(fi)
        current_app.g['zabbix_update'] = current_mtime
        zbx_log.info('refreshed_zabbix_data', epoch=time.time(), mtime=current_mtime)


@zabbix_api.route('/')
def zabbix_index():
    return jsonify(list(current_app.g['zabbix'].keys()))


@zabbix_api.route('/<string:cluster>')
def zabbix_cluster(cluster):
    return jsonify(current_app.g['zabbix'][cluster])


def create_api_app(host='0.0.0.0', port=5000, disc='discovery.json'):
    app = Flask(__name__)
    app.register_blueprint(zabbix_api)
    app.register_blueprint(lag_api)
    app.g = {
        'disc': disc,
        'zabbix': None,
        'zabbix_update': None,
        'lag': None,
        'lag_update': None
    }
    wsgi_server = WSGIServer((host, port), app, log=None)

    # TODO: fix empty discovery lookup junk, just ignore zabbix if it's not
    # present

    api_log.info('discovering_api_files')
    files = ['discovery.json', 'server.json']
    while files:
        for fi in files:
            if os.path.exists(os.path.join(disc, fi)):
                api_log.info('found_api_file', file=fi)
                files.remove(fi)

    return wsgi_server
