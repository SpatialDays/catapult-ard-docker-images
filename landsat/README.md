# ARD Workflow Container for Landsat OLI, ETM & TM Datasets

## Docker Hub images
Pre-built Docker images for production use can be pulled from [our Docker Hub repo](https://hub.docker.com/r/satapps/).

## Dockerfile for development
The provided [Dockerfile](Dockerfile-devel) creates a Docker image with necessary packages for running an ARD workflow for Landsat OLI, ETM & TM datasets, set up by means of Miniconda v4.7.10. [Jupyter Notebook](https://jupyter.org/) is included for interactive development and started once the Docker image is run.

## Docker Compose
A [Docker Compose](docker-compose.yml) example file is provided to set up an interactive ARD workflow instance for development purposes.

## Environment variables

|Env var|Used for|Default
|---|---|---|
|REDIS_HOST|Host name for redis queue.|localhost|
|REDIS_PORT|Port for redis queue.|6379|
|REDIS_USGS_PROCESSED_CHANNEL|Redis list name for incoming imagery (payload which contains USGS download links).|jobLS|
|AWS_ACCESS_KEY_ID | AWS access key.|n/a|
|AWS_SECRET_ACCESS_KEY | AWS secret key.|n/a|
|AWS_DEFAULT_REGION | AWS region.|n/a|
|S3_ENDPOINT_URL | S3 endpoint url.|n/a|
|S3_BUCKET | S3 bucket name.|n/a|

<!-- ### Environment variables for Docker Compose
Environment variables should be set in a `.env` file for Docker Compose. You might use [.env.example](./.env.example) as a starting point. The [.gitignore](../.gitignore) file contains an entry for `.env` in order to avoid it from being accidentally added to this repository, so the `.env` file is suitable for storing sensitive information. -->

### Building and running a development platform
Set up an ARD workflow instance by issuing:

```
docker-compose up -d
```

Once the above completes, the job queue is ready to be filled in with work items by issuing:

```bash
docker exec -it redis-master /bin/bash
redis-cli -h redis-master
rpush jobLS '{"in_scene": "https://edclpdsftp.cr.usgs.gov/orders/espa-tom.jones@sa.catapult.org.uk-08132020-074750-955/LT050800731990092001T2-SC20200813131002.tar.gz", "s3_bucket": "public-eo-data", "s3_dir": "test/landsat_5/", "item":""}'
...
lrange jobS2 0 -1
```

For [mass insertion](https://redis.io/topics/mass-insert) you can use e.g.:

```bash
docker exec -it redis-master /bin/bash
cat <<EOF | redis-cli -h redis-master --pipe
rpush jobLS '{"in_scene": "https://edclpdsftp.cr.usgs.gov/orders/espa-tom.jones@sa.catapult.org.uk-08132020-074750-955/LT050800731990092001T2-SC20200813131002.tar.gz", "s3_bucket": "public-eo-data", "s3_dir": "test/landsat_5/","item":""}'
...
EOF
```

At any time afterwards, the queue can be processed interactively by running the worker Jupyter Notebook.

<!-- ### Jupyter Notebook
Jupyter Notebook can be accessed at the URL: http://{Serve's IP Address}:8899.\
The access token is `secretpassword`, which is set by means of the CMD statement within the [Dockerfile](Dockerfile). -->

<!-- ### Amending the workflow
The actual workflow can be developed within the [ard-workflows](https://github.com/SatelliteApplicationsCatapult/ard-workflows) submodule at workflows directory. -->

<!-- ## TODO
- Define the `PLATFORM` and `QUEUE_NAME` environment variables, so these can be set to `SENTINEL_2` and `jobS2` respectively, making the worker code agnostic of the satellite/platform to work on
- Define the `LEASE_SECS` and `TIMEOUT` environment variables, so these can be set according to what is appropriate for the satellite/platform to work on; alternatively read defaults from a configuration file that can be provided as an `env_file` in Docker Compose or as a `ConfigMap` in Kubernetes
- Generate a single Docker image: Jupyter Notebook could be optionally installed upon deployment, based on an environment variable, e.g. `JUPYTER_NOTEBOOK` set to `YES`; the main drawback of doing so (compared to building separate Docker images) is that dependencies might fail to support the installation of Jupyter Notebook 
- Evaluate the use of [RQ](https://python-rq.org/), [Celery](http://www.celeryproject.org/), or [pyres](https://github.com/binarydud/pyres) for implementing a more resilient work queue -->
