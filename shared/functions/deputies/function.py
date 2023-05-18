import logging
import os.path

from dependency_injector.wiring import inject, Provide

from .actors import actors_task
from .organs import organs_task
from ...components import JobsTable
from ...env import Environment
from ...utils import download_file, now_with_tz

JOB_PARTITION_KEY = "deputies"


@inject
def deputies(jobs: JobsTable = Provide["gateways.jobs_table"]) -> None:
    last_run = jobs.get_last_run(JOB_PARTITION_KEY).get("run_datetime")
    logging.info(f"[{JOB_PARTITION_KEY.upper()} Job] - Last run was on {last_run}")

    logging.info(f"Downloading deputies, mandates and organs from {Environment.deputies_url}...")
    data_dir = download_file(Environment.deputies_url, auto_extract=True)
    #data_dir = "/home/pbreton/Downloads/AMO10_deputes_actifs_mandats_actifs_organes.json"
    actors_task(os.path.join(data_dir, "json", "acteur"))
    organs_task(os.path.join(data_dir, "json", "organe"))
    # TODO: add deports
    # deports_task(os.path.join(data_dir, "json", "deport"))

    jobs.update_last_run(JOB_PARTITION_KEY, run_datetime=now_with_tz())
    logging.info("Done")
