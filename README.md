# Development environment for Airflow, Dagster and Prefect

# Setup
In order to run all of the tasks, a few servers have to be up and running. To set up all the necessary servers, do the following setps.

## Minio setup
Run this command (and keep it running in the background):

    /minio server /mnt/data --console-address ":9001"

This will setup an S3 storage that you can access in the browser at localhost:9000.

## NATS setup
Run this command (and keep it running in the background):

    nats-server -js

This will setup a stream service that you can publish messages to. But you first have to add a stream to the service. Do it by executing this command:

    nats stream add ais_stream --subjects "ais" --storage file --replicas 1 --retention limits --discard old --max-msgs=-1 --max-msgs-per-subject=-1 --max-bytes=-1 --max-age=-1 --max-msg-size=-1 --dupe-window="2m0s" --no-allow-rollup --no-deny-delete --no-deny-purge

# Dagster
To run dagster, cd into the directory test_dagster (with the workspace.yaml file) and execute this command:

    dagit

In order to run and schedule you jobs, you also need to have the dagster-daemon process running in the background. To do this, make sure you are in the same directory as the workspace.yaml file and execute this command:

    dagster-daemon run

# Airflow
To run airflow, just go the test_airflow folder and execute this command:

    airflow standalone

This will start up all the necessary processes (scheduler, UI, executor)

# Prefect
To run prefect, navigate to the test_prefect folder and execute this command:

### Setup API + UI
    prefect orion start

### Setup Agent (for scheduling)

    prefect agent start -q ais_stream

### Create deployments

    prefect deployment build dump_to_s3.py:dump_to_s3 -n dump_to_s3 -q ais_queue


