from airflow import DAG
from airflow.sdk.definitions.decorators import task
from datetime import datetime
import requests
import json

# ✔ REPLACE THIS with your webhook
GOOGLE_CHAT_WEBHOOK = "https://chat.googleapis.com/v1/spaces/AAQAbrTjw08/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=qnXsngkbXjbXL6K_1--mD4uXVKbie4T2uEHBxXOInm0"


DEFAULT_EMAIL = "nagavarunkumar.parvathareddy@coartha.com"


# -----------------------------------------------------------------
# Helper: Send message to Google Chat
# -----------------------------------------------------------------
def send_chat_message(text: str):
    message = {"text": text}

    response = requests.post(
        GOOGLE_CHAT_WEBHOOK,
        data=json.dumps(message),
        headers={"Content-Type": "application/json"},
    )
    response.raise_for_status()


# -----------------------------------------------------------------
# Failure Callback (Google Chat alert on failure)
# -----------------------------------------------------------------
def failure_callback(context):
    task_id = context["task_instance"].task_id
    dag_id = context["dag"].dag_id
    error = context["exception"]

    send_chat_message(
        f"❌ *Airflow Task Failed*\n"
        f"*DAG:* {dag_id}\n"
        f"*Task:* {task_id}\n"
        f"*Error:* {error}"
    )


# -----------------------------------------------------------------
# DAG Definition
# -----------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "email": [DEFAULT_EMAIL],      # email on failure
    "email_on_failure": True,
    "email_on_retry": False,
    "on_failure_callback": failure_callback,   # Google Chat failure alert
}

with DAG(
    dag_id="parser_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["parser"],
):

    @task
    def resumes_received():
        send_chat_message("Parsers - 📥 *Resumes Received*")
        return "OK"

    @task
    def resumes_parsed():
        send_chat_message("Parsers - 📄 *Resumes Parsed*")
        return "OK"

    @task
    def resumes_enriched():
        send_chat_message("Parsers -✨ *Resumes Enriched*")
        return "OK"

    t1 = resumes_received()
    t2 = resumes_parsed()
    t3 = resumes_enriched()

    t1 >> t2 >> t3
