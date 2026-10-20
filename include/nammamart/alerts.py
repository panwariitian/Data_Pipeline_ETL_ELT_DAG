"""Failure alert: a Slack message whenever a task in a NammaMart DAG fails.

The webhook URL is an Airflow Variable (Admin > Variables, key SLACK_WEBHOOK_URL), never a value in code.
With no variable set the alert is logged and skipped, so a missing secret cannot fail a run.
"""
import json
import logging
import urllib.request

from airflow.sdk import Variable

log = logging.getLogger(__name__)
WEBHOOK_VARIABLE = "SLACK_WEBHOOK_URL"


def notify_failure(context) -> None:
    ti = context["task_instance"]
    text = (f":red_circle: NammaMart pipeline failed\n"
            f"DAG: {ti.dag_id}  task: {ti.task_id}  try: {ti.try_number}\n"
            f"run: {context['dag_run'].run_id}\n"
            f"error: {str(context.get('exception'))[:500]}")
    url = Variable.get(WEBHOOK_VARIABLE, default=None)
    if not url:
        log.warning("%s is not set; failure alert not sent:\n%s", WEBHOOK_VARIABLE, text)
        return
    try:
        req = urllib.request.Request(url, data=json.dumps({"text": text}).encode(),
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            log.info("failure alert sent to Slack: HTTP %s", resp.status)
    except Exception as exc:  # an alert problem must never hide the original failure
        log.error("failure alert could not be sent: %s", exc)
