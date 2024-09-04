# apscheduler_config.py
import os
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor

def get_scheduler_config():
    """
    Returns the configuration settings for APScheduler.
    """
    return {
        'apscheduler.timezone': 'UTC',  # Set the timezone for the scheduler
        'apscheduler.executors.default': ThreadPoolExecutor(10),  # Default thread pool executor
        'apscheduler.executors.processpool': ProcessPoolExecutor(5),  # Process pool executor for CPU-bound tasks
        'apscheduler.job_defaults.coalesce': False,  # Whether to coalesce (merge) missed job executions
        'apscheduler.job_defaults.max_instances': 3,  # Maximum number of concurrent job instances
    }

def create_scheduler():
    """
    Creates and returns a configured BackgroundScheduler instance.
    """
    config = get_scheduler_config()
    scheduler = BackgroundScheduler()
    scheduler.configure(config)
    return scheduler
