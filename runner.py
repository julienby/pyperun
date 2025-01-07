from joblib import Parallel, delayed
import logging

logger = logging.getLogger()

def run_pipelines_in_parallel(pipelines):
    """
    Exécute plusieurs pipelines en parallèle.
    :param pipelines: Liste de fonctions de pipeline
    """
    logger.info("Starting parallel execution of pipelines")
    Parallel(n_jobs=len(pipelines))(delayed(pipeline)() for pipeline in pipelines)
    logger.info("Parallel execution of pipelines completed")

