import logging
import os

import azure.functions as func

from datetime import datetime, timedelta, timezone

from lib.google import Google
from lib.key_vault import KeyVault
from lib.log_analytics import LogAnalytics
from lib.storage import Storage


# Google API scope
GOOGLE_SCOPES = ["https://www.googleapis.com/auth/apps.alerts"]
# Interval of script execution
SCRIPT_EXECUTION_INTERVAL_MINUTES = 10
# Environment variables
ENV = (
    "DCE_URL",
    "DCR_ID",
    "DCR_STREAM",
    "KV_SECRET_GOOGLE_SERVICE_ACCOUNT",
    "KV_SECRET_GOOGLE_USER",
    "KV_URL",
    "STORAGE_BLOB_FILE",
    "STORAGE_CONTAINER",
    "STORAGE_NAME",
)


def get_query_api_date(storage):
    logging.info("Getting last query date")

    last_query_date = storage.get()
    now = datetime.now(timezone.utc).replace(tzinfo=timezone.utc)

    # Check if we have the state file with the last query date within it
    if last_query_date:
        logging.info(f"Last query date - {last_query_date}")
    else:
        last_query_date = now - timedelta(minutes=SCRIPT_EXECUTION_INTERVAL_MINUTES)
        logging.info("Last query date is not known")

    # Minus 1 minute to let API provider publish logs
    endtime = now - timedelta(minutes=1)

    logging.info(f"Getting data from {last_query_date} to {endtime}")

    # Update state file with the end time of the current query
    # This one will be the start time for the next launch
    storage.post(endtime.isoformat())

    return last_query_date, endtime.isoformat()


def check_env(env_vars):
    vars = {}

    for env in env_vars:
        env_value = os.environ.get(env)
        if not env_value:
            raise ValueError(f"Environment variable {env} not set")
        else:
            vars[env] = env_value

    return vars


app = func.FunctionApp()


@app.function_name(name="GoogleAlertCenter")
@app.timer_trigger(
    schedule="0 */10 * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def GoogleAlertsSentinelConnector(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info("The timer is past due!")

    utc_timestamp = datetime.now(timezone.utc).replace(tzinfo=timezone.utc).isoformat()

    logging.info(f"Python timer trigger function ran at {utc_timestamp}")
    logging.info("Starting program")

    vars = check_env(ENV)

    storage = Storage(
        vars["STORAGE_NAME"],
        vars["STORAGE_CONTAINER"],
        vars["STORAGE_BLOB_FILE"],
    )

    start_time, end_time = get_query_api_date(storage)

    kv = KeyVault(vars["KV_URL"])
    google_service_account = kv.get_secret(vars["KV_SECRET_GOOGLE_SERVICE_ACCOUNT"])
    google_user = kv.get_secret(vars["KV_SECRET_GOOGLE_USER"])

    google = Google(google_service_account, GOOGLE_SCOPES, google_user)
    alerts = google.get_alerts(start_time, end_time)

    logging.info(f"Number of alerts - {len(alerts)}")

    log_analytics = LogAnalytics(
        vars["DCE_URL"],
        vars["DCR_ID"],
        vars["DCR_STREAM"],
    )
    log_analytics.upload(alerts)
