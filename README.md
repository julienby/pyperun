# **Python Pipeline Scheduler with Flask and APScheduler**

A simple Python-based pipeline scheduler that allows the execution of multiple pipelines (composed of one or more tasks) in parallel. It also includes a minimalistic Flask dashboard to monitor task execution history and scheduled pipelines.

---

## **Features**

- **Pipeline Scheduling:**
  - Schedule pipelines with **APScheduler** using intervals or specific dates/times.
  - Pipelines can be composed of one or more sequential tasks.

- **Parallel Execution:**
  - Pipelines can run in parallel using **Joblib** for efficient CPU utilization.

- **Flask Dashboard:**
  - A lightweight dashboard to monitor:
    - Execution history of tasks and pipelines.
    - List of pipelines currently scheduled.

- **Database:**
  - Uses **SQLite** to store the execution history of tasks and pipelines.

---

## **Project Structure**

```plaintext
project/
├── main.py                  # Entry point of the program
├── tasks.py                 # Definition of tasks and pipelines
├── runner.py                # Executes pipelines in parallel
├── scheduler.py             # Schedules pipelines using APScheduler
├── flask_app.py             # Flask dashboard for monitoring
├── database.py              # SQLite management for task history
└── templates/
    └── index.html           # HTML template for the Flask dashboard
```

---

## **Getting Started**

### **1. Prerequisites**

Ensure you have Python 3.8 or later installed.

Install the required Python dependencies:
```bash
pip install flask apscheduler joblib
```

### **2. Running the Application**

Start the application by running:
```bash
python main.py
```

The scheduler and Flask dashboard will run simultaneously.

---

## **Usage**

### **1. Adding Tasks and Pipelines**

- Define tasks and pipelines in `tasks.py`:
  ```python
  def task_1():
      print("Task 1 is running...")
  
  def pipeline_1():
      task_1()
      print("Pipeline 1 is complete!")
  ```

- Pipelines are automatically detected if their names follow the convention `pipeline_<name>`.

### **2. Scheduling Pipelines**

Schedule pipelines dynamically in `scheduler.py`:
```python
scheduler.add_job(pipelines["pipeline_1"], "interval", minutes=15)  # Run every 15 minutes
scheduler.add_job(pipelines["pipeline_2"], "date", run_date="2025-01-10 10:30:00")  # Run at a specific time
```

### **3. Flask Dashboard**

- Open the dashboard in your browser at: [http://localhost:5000](http://localhost:5000)
- Available routes:
  - `/`: View task execution history.
  - `/api/pipelines`: View available pipelines.
  - `/api/history`: View task history in JSON format.

---

## **Project Details**

### **Database**

- The database (`task_status.db`) uses SQLite and is initialized in `database.py`.
- The `task_status` table tracks:
  - Task name
  - Pipeline name
  - Status (`success` or `failed`)
  - Timestamp of the last run

### **Execution Flow**

1. **Task Execution:**
   - Tasks are functions defined in `tasks.py`.
   - Tasks are sequentially executed within a pipeline.

2. **Pipeline Scheduling:**
   - Pipelines are dynamically discovered using Python's `globals()` or `vars()`.
   - Scheduling is handled via APScheduler.

3. **Parallel Execution:**
   - Multiple pipelines can run simultaneously using Joblib.

---

## **Customization**

- To add a new task or pipeline:
  - Define the task or pipeline in `tasks.py`.
  - Pipelines must follow the naming convention `pipeline_<name>` to be auto-discovered.

- To customize scheduling:
  - Update `scheduler.py` to add or modify pipeline schedules.

---

## **Future Improvements**

- Add user authentication to secure the Flask dashboard.
- Add retry logic for failed tasks.
- Enhance the dashboard with more statistics (e.g., pipeline run durations).
- Support dynamic pipeline creation via the dashboard.

---

## **License**

This project is licensed under the MIT License.

---

## **Contributing**

Contributions are welcome! Feel free to fork this repository, create a feature branch, and submit a pull request.
