# -----------------------------------------------------

# for parsl_workflow.py:

# base image could be python 9 or 10
FROM python:3.10

# the following line may not work bc have not built image from docker file in this repo yet?
#LABEL org.opencontainers.image.source https://github.com/PermafrostDiscoveryGateway/viz-workflow

WORKDIR /home/jcohen/viz-worflow/docker-parsl_workflow/app

COPY docker-parsl-workflow/parsl_workflow.py .
COPY docker-parsl-workflow/parsl_config.py .
COPY docker-parsl-workflow/viz_config.json .
COPY docker-parsl-workflow/requirements.txt .

# import 6 UTM zones of lake change data to process in parallel
COPY /var/data/submission/pdg/nitze_lake_change/data_2022-11-04/lake_change_GD_cleaned/cleaned_files/data_products_32635-32640 .

RUN pip install -r requirements.txt

# maybe we don't want to run a command bc we need to use the terminal to
# do it now that we are using parsl and k8s
# CMD [ "python", "./parsl_workflow.py" ]

