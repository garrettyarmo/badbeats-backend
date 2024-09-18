from datetime import datetime
from fastapi import FastAPI
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from services.data_ingestion import (
    ingest_timeframe_data,
    ingest_teams_data,
    ingest_players_data
)
from services.schema_setup import (
    setup_timeframes_table,
    setup_teams_table,
    setup_players_table
)

app = FastAPI()
scheduler = BackgroundScheduler()

tasks = {
    "weekly": {
        "interval": 604800,  # Every week (in seconds)
        "task": [ingest_timeframe_data, ingest_teams_data, ingest_players_data]  # ADD WEEKLY TASKS HERE
    }
}

@app.on_event("startup")
def start_scheduler():
    print("***Starting Application***")
    
    # Set up the database schemas
    print("Setting up database schemas...")
    setup_timeframes_table()
    setup_teams_table()
    setup_players_table()
    
    # Start the scheduler
    print("***Started Scheduler***")
    for task_name, task_info in tasks.items():
        if task_info["task"]:
            print(f'\nScheduling {task_name} pipelines...')
            for task in task_info["task"]:  # Loop through each task in the list
                scheduler.add_job(
                    task,  # Individual task function
                    IntervalTrigger(seconds=task_info["interval"]),
                    id=f"{task_name}_{task.__name__}",  # Unique ID for each task
                    name=f"Task: {task.__name__}",
                    next_run_time=datetime.now()
                )
                print(f'Scheduled {task.__name__} every {task_info["interval"]} seconds!')
        else:
            print(f'Error: No tasks defined for {task_name}.')
    
    scheduler.start()