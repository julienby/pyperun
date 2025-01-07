import time
from database import save_task_status
import logging

logger = logging.getLogger()

# Tâches individuelles
def task_1():
    time.sleep(2)
    logger.info("Task 1 completed")
    save_task_status("Task 1", "Pipeline 1", "success")

def task_2():
    time.sleep(3)
    logger.info("Task 2 completed")
    save_task_status("Task 2", "Pipeline 1", "success")

def task_3():
    time.sleep(1)
    logger.info("Task 3 completed")
    save_task_status("Task 3", "Pipeline 1", "success")

def task_4():
    time.sleep(2)
    logger.info("Task 4 completed")
    save_task_status("Task 4", "Pipeline 2", "success")

def task_5():
    time.sleep(3)
    logger.info("Task 5 completed")
    save_task_status("Task 5", "Pipeline 2", "success")

# Pipelines
def pipeline_1():
    logger.info("Starting Pipeline 1")
    task_1()
    task_2()
    task_3()
    logger.info("Pipeline 1 completed")

def pipeline_2():
    logger.info("Starting Pipeline 2")
    task_4()
    task_5()
    logger.info("Pipeline 2 completed")

