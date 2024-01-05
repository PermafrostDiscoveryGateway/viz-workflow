# -----------------------------------------------------

# for parsl_workflow.py:

# base image could be python 9 or 10
FROM python:3.10

# the following line may not work bc have not built image from docker file in this repo yet?
#LABEL org.opencontainers.image.source https://github.com/PermafrostDiscoveryGateway/viz-workflow

WORKDIR /home/jcohen/viz-worflow/docker-parsl_workflow/app

COPY parsl_workflow.py .
COPY parsl_config.py .
COPY viz_config.json .
COPY requirements.txt .

# import 1 UTM zone of lake change data to process in parallel until can figure out how to transfer an entire dir of 6 UTM zones
COPY lake_change_cleaned.gpkg .

RUN pip install -r requirements.txt

# maybe we don't want to run a command bc we need to use the terminal to
# do it now that we are using parsl and k8s
# CMD [ "python", "./parsl_workflow.py" ]

