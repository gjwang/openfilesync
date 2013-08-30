## Broker settings.
#BROKER_URL = "amqp://guest:guest@localhost:5672//"
BROKER_URL = "redis://"

# List of modules to import when celery starts.
CELERY_IMPORTS = ("celeryfilesync.tasks", )

## Using the database to store task state and results.
#CELERY_RESULT_BACKEND = "database"
#CELERY_RESULT_DBURI = "sqlite:///mydatabase.db"

CELERY_ANNOTATIONS = {"tasks.add": {"rate_limit": "10/s"}}
