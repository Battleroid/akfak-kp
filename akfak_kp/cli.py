import click
import structlog
import json


from akfak_kp import __version__
from akfak_kp.akfak import AkfakServer
from akfak_kp.api import create_api_app
from akfak_kp.util import load_config, build_discovery_playlist
from akfak_kp.log import setup_logging


setup_logging('ERROR')
cli_log = structlog.get_logger('akfak.cli')


class Config:
    def __init__(self, path='config.yaml'):
        self.path = path
        self.config = load_config(path)


@click.group()
@click.version_option(__version__)
def cli():
    """
    Akfak does the basic monitoring of lag for topic:consumer pairs, as well
    as serving the files from the server portion as a primitive API (intended
    for use in Kubernetes).
    """
    pass


@cli.command()
def monitor():
    """
    Show lag info for topic:consumer pairs in console.
    """
    raise NotImplementedError


# TODO: need option to ignore zabbix discovery portion
@cli.command()
@click.option('--host', default='0.0.0.0', type=str, show_default=True)
@click.option('--port', default=5000, type=int, show_default=True)
@click.option(
    '--disc',
    default='./',
    type=click.Path(exists=True, dir_okay=True, file_okay=False),
    help='Directory to watch for discovery & lag output'
)
def api(host, port, disc):
    """
    Serve API from server portion of Akfak.
    """
    wsgi_server = create_api_app(host, port, disc)
    wsgi_server.serve_forever()


@cli.group()
@click.option(
    '--config',
    default='config.yaml',
    type=click.Path(exists=True, dir_okay=False)
)
@click.pass_context
def server(ctx, config):
    """
    Fetch offsets, dump results to file. Perform Zabbix rule evaluations
    in results if configured.
    """
    ctx.obj = Config(config)


@server.command()
@click.pass_obj
def zabbix(config):
    """
    Dump zabbix auto discovery list for debugging purposes.
    """
    print(json.dumps(
          build_discovery_playlist(config.config),
          indent=2,
          sort_keys=True))


@server.command()
@click.pass_obj
def config(config):
    """
    Dump completed config for debugging purposes.
    """
    import yaml
    yaml.Dumper.ignore_aliases = lambda *args: True
    print(yaml.dump(
        config.config,
        default_flow_style=False,
        explicit_start=True
    ))


@server.command()
@click.pass_obj
def start(config):
    """
    Start fetching and sending.
    """
    ak = AkfakServer(config.path)
    try:
        ak.start()
    except (KeyboardInterrupt, SystemExit):
        ak.stop()
    except Exception as e:
        cli_log.error('unexpected_error', exc_info=e)
    finally:
        if not ak.done:
            ak.stop()


if __name__ == '__main__':
    cli()
