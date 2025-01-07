from apscheduler.schedulers.background import BackgroundScheduler
import tasks  # Import des pipelines et tâches
import logging

logger = logging.getLogger()

def discover_pipelines():
    """
    Découvre automatiquement tous les pipelines définis dans le module `tasks`.
    Les pipelines sont identifiés comme des fonctions dont le nom commence par `pipeline_`.
    """
    return {name: func for name, func in vars(tasks).items() if callable(func) and name.startswith("pipeline_")}

def setup_scheduler():
    """
    Configure le scheduler pour planifier des pipelines détectés dynamiquement.
    """
    scheduler = BackgroundScheduler()

    # Découverte dynamique des pipelines
    pipelines = discover_pipelines()

    # Exemple de planification : Ajout des pipelines détectés
    scheduler.add_job(pipelines["pipeline_1"], "interval", seconds=60, id="pipeline_1")
    scheduler.add_job(pipelines["pipeline_2"], "date", run_date="2025-01-07 00:51:30", id="pipeline_2")

    scheduler.start()
    logger.info(f"Scheduler started with pipelines: {list(pipelines.keys())}")
    return scheduler

