import logging.config

import click

from feast_spark.job_service import start_job_service

logging.config.dictConfig(
    {
        "version": 1,
        "disable_existing_loggers": True,
        "formatters": {
            "standard": {"format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"},
        },
        "handlers": {
            "debug": {
                "level": "DEBUG",
                "formatter": "standard",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
            },
            "standard": {
                "level": "WARNING",
                "formatter": "standard",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stderr",
            },
        },
        "loggers": {
            "": {"handlers": ["standard"], "level": "WARNING", "propagate": False},
            "feast_spark": {
                "handlers": ["debug", "standard"],
                "level": "INFO",
                "propagate": False,
            },
            "feast": {
                "handlers": ["debug", "standard"],
                "level": "INFO",
                "propagate": False,
            },
        },
    }
)


@click.group()
def cli():
    pass


@cli.command(name="server")
def server():
    """
    Start Feast Job Service
    """
    start_job_service()


if __name__ == "__main__":
    cli()
