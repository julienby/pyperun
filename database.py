import sqlite3
from datetime import datetime

DATABASE = 'task_status.db'

def init_db():
    conn = sqlite3.connect(DATABASE)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS task_status (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_name TEXT,
            pipeline_name TEXT,
            status TEXT,
            last_run TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

def save_task_status(task_name, pipeline_name, status):
    """
    Enregistre le statut d'une tâche dans la base de données.
    :param task_name: Nom de la tâche
    :param pipeline_name: Nom du pipeline
    :param status: Statut de la tâche (success/failed)
    """
    conn = sqlite3.connect(DATABASE)
    c = conn.cursor()
    c.execute('''
        INSERT INTO task_status (task_name, pipeline_name, status, last_run)
        VALUES (?, ?, ?, ?)
    ''', (task_name, pipeline_name, status, datetime.now()))
    conn.commit()
    conn.close()


def get_task_history(limit=100):
    conn = sqlite3.connect(DATABASE)
    c = conn.cursor()
    c.execute('SELECT task_name, pipeline_name, status, last_run FROM task_status ORDER BY last_run DESC LIMIT ?', (limit,))
    rows = c.fetchall()
    conn.close()
    return rows

