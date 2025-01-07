from flask import Flask, jsonify, render_template
from database import get_task_history
from scheduler import discover_pipelines
import logging

logger = logging.getLogger()

# Créer l'application Flask
app = Flask(__name__)

@app.route("/")
def home():
    """
    Page principale affichant l'historique des tâches.
    """
    history = get_task_history()
    return render_template("index.html", history=history)

@app.route("/api/pipelines")
def api_pipelines():
    """
    API pour lister les pipelines disponibles.
    """
    pipelines = discover_pipelines()
    return jsonify(list(pipelines.keys()))

@app.route("/api/history")
def api_history():
    """
    API pour récupérer l'historique des tâches.
    """
    history = get_task_history()
    return jsonify(history)

if __name__ == "__main__":
    app.run(debug=True)

