import time  # Ajoutez cette ligne

from scheduler import setup_scheduler
from flask_app import app
from database import init_db
import logging
import threading

# Configurer le logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger()

if __name__ == "__main__":
    # Initialiser la base de données
    init_db()

    # Configurer le scheduler
    scheduler = setup_scheduler()

    # Lancer Flask dans un thread séparé
    def run_flask():
        app.run(debug=True, use_reloader=False)  # Flask sans rechargement automatique pour éviter les doublons

    flask_thread = threading.Thread(target=run_flask)
    flask_thread.start()

    # Garder le programme en cours d'exécution
    try:
        logger.info("Application is running. Press Ctrl+C to exit.")
        while True:
            time.sleep(1)  # Ajoute un délai d'une seconde pour réduire l'utilisation CPU
            pass
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("Scheduler stopped")

