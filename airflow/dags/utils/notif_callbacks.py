import logging
import requests
from airflow.hooks.base import BaseHook

TELEGRAM_CHAT_ID = "6773967052"

def consistent_notif_message(context, **kwargs):
    """
    Sends Telegram message when the data is consistent
    """
    task_instance = context["task_instance"]
    task_id = task_instance.task_id
    run_id = context["run_id"]
    dag_id = context["dag"].dag_id
    dag_display_name = dag_id.replace("_", " ").title()

    if "manual__" in run_id:
        execution_date = run_id.replace("manual__", "").split("T")[0]
        execution_time = run_id.replace("manual__", "").split("T")[1].split("+")[0][:8]
        run_display = f"Manual Run - {execution_date} {execution_time}"
    else:
        run_display = run_id

    message = (
        f"🎉 <b>DAG Execution Successful</b> 🎉\n\n"
        f"📋 <b>DAG:</b> {dag_display_name}\n"
        f"📊 <b>Task:</b> {task_id}\n"
        f"🚀 <b>Run:</b> {run_display}\n"
        f"📥 <b>Rows Ingested:</b> {kwargs.get('ingested', 'N/A')}\n"
        f"📤 <b>Rows Loaded:</b> {kwargs.get('loaded', 'N/A')}\n"
        f"✅ <b>Status:</b> Data Consistent\n\n"
        f"<i>All data successfully transferred and validated!</i>"
    )

    try:
        telegram_conn = BaseHook.get_connection("telegram_default")
        bot_token = telegram_conn.password

        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }
        
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        
        logging.info(f"Telegram alert sent successfully for consistent DAG: {dag_id}")
    
    except Exception as e:
        logging.warning(f"Telegram alert failed: {e}")

def inconsistent_notif_message(context, **kwargs):
    """
    Sends Telegram message when the data is inconsistent
    """
    task_instance = context["task_instance"]
    task_id = task_instance.task_id
    run_id = context["run_id"]
    dag_id = context["dag"].dag_id
    dag_display_name = dag_id.replace("_", " ").title()

    if "manual__" in run_id:
        execution_date = run_id.replace("manual__", "").split("T")[0]
        execution_time = run_id.replace("manual__", "").split("T")[1].split("+")[0][:8]
        run_display = f"Manual Run - {execution_date} {execution_time}"
    else:
        run_display = run_id

    message = (
        f"⚠️ <b>Data Inconsistency Detected</b> ⚠️\n\n"
        f"📋 <b>DAG:</b> {dag_display_name}\n"
        f"📊 <b>Task:</b> {task_id}\n"
        f"🚀 <b>Run:</b> {run_display}\n"
        f"📥 <b>Rows Ingested:</b> {kwargs.get('ingested', 'N/A')}\n"
        f"📤 <b>Rows Loaded:</b> {kwargs.get('loaded', 'N/A')}\n"
        f"❌ <b>Status:</b> Data Inconsistent\n\n"
        f"<i>Please check the data pipeline immediately!</i>"
    )

    try:
        telegram_conn = BaseHook.get_connection("telegram_default")
        bot_token = telegram_conn.password

        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }
        
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        
        logging.info(f"Telegram alert sent successfully for inconsistent DAG: {dag_id}")
    
    except Exception as e:
        logging.warning(f"Telegram alert failed: {e}")

def dag_success_notif_message(**context):
    """
    Sends Telegram message when the DAG is successful
    """
    run_id = context["run_id"]
    dag_id = context["dag"].dag_id
    dag_display_name = dag_id.replace("_", " ").title()

    if "manual__" in run_id:
        execution_date = run_id.replace("manual__", "").split("T")[0]
        execution_time = run_id.replace("manual__", "").split("T")[1].split("+")[0][:8]
        run_display = f"Manual Run - {execution_date} {execution_time}"
    else:
        run_display = run_id

    message = (
        f"🎉 <b>DAG Execution Successful</b> 🎉\n\n"
        f"📋 <b>DAG:</b> {dag_display_name}\n"
        f"🚀 <b>Run:</b> {run_display}\n"
        f"✅ <b>Status:</b> DAG Successful\n\n"
        f"<i>All data successfully inserted into the database!</i>"
    )

    try:
        telegram_conn = BaseHook.get_connection("telegram_default")
        bot_token = telegram_conn.password

        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }
        
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()

        logging.info(f"Telegram alert sent successfully for DAG: {dag_id}")

    except Exception as e:
        logging.warning(f"Telegram alert failed: {e}")

def failed_notif_message(context):
    """
    Sends Telegram message when the task fails
    """
    task_instance = context["task_instance"]
    task_id = task_instance.task_id
    run_id = context["run_id"]
    dag_id = context["dag"].dag_id
    dag_display_name = dag_id.replace("_", " ").title()

    exception = context.get("exception")
    error_message = str(exception) if exception else "Unknown Error"

    if "manual__" in run_id:
        execution_date = run_id.replace("manual__", "").split("T")[0]
        execution_time = run_id.replace("manual__", "").split("T")[1].split("+")[0][:8]
        run_display = f"Manual Run - {execution_date} {execution_time}"
    else:
        run_display = run_id

    message = (
        f"🚨 <b>DAG Execution Failed</b> 🚨\n\n"
        f"📋 <b>DAG:</b> {dag_display_name}\n"
        f"📊 <b>Task:</b> {task_id}\n"
        f"🚀 <b>Run:</b> {run_display}\n"
        f"❌ <b>Error:</b> {error_message}\n\n"
        f"<i>Please investigate and resolve the issue!</i>"
    )

    try:
        telegram_conn = BaseHook.get_connection("telegram_default")
        bot_token = telegram_conn.password
        
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }
        
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        
        logging.info(f"Telegram alert sent successfully for failed task: {task_id}")
        
    except Exception as e:
        logging.warning(f"Telegram alert failed: {e}")