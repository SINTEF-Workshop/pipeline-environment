# Development environment for Airflow, Dagster and Prefect
In this tutorial, we will be working with a pipelines that collects stream data from Barentswatch Live Data API. The pipeline is implemented in all three tools.

# Setup
In order to run all of the tasks, a few servers have to be up and running. To set up all the necessary servers, do the following setps.

## NATS setup
Since we will be working with stream data, we need a way to manage this. Nats is a streaming service for this particular purpose.

You first have to add a stream to the service. Do it by executing this command:

    nats stream add ais_stream --subjects "ais" --storage file --replicas 1 --retention limits --discard old --max-msgs=-1 --max-msgs-per-subject=-1 --max-bytes=-1 --max-age=-1 --max-msg-size=-1 --dupe-window="2m0s" --no-allow-rollup --no-deny-delete --no-deny-purge

# Dagster
To run dagster, cd into the directory dagster_ais (with the workspace.yaml file) and execute this command:

    dagit

You may notice a warning icon next to the "Daemons" section, and navigating to this will allow you to see several servers that are not currently running. In order to schedule jobs, you will need to have the scheduler process running in the background. To do this, make sure you are in the same directory as the workspace.yaml file and execute this command:

    dagster-daemon run

Hopefully, the servers will be running, and the warning icon will disappear.

# Airflow
To run airflow, just go the airflow_ais folder and execute this command:

    airflow standalone

This will start up all the necessary processes (scheduler, UI, executor)

# Prefect
To run prefect, navigate to the prefect_ais folder and execute these commands (make sure you are in the same folder for all of the commands):

### Setup API + UI
    prefect orion start

### Create deployments

    prefect deployment build dump_to_s3.py:dump_to_s3 -n dump_to_s3 -q ais_queue --apply

### Setup Agent (for scheduling)

    prefect agent start -q ais_queue