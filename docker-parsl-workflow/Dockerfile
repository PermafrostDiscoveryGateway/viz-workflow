# for parsl_workflow.py:

FROM python:3.9

LABEL org.opencontainers.image.source https://github.com/permafrostdiscoverygateway/viz-workflow

# note: is it necessary to start WORKDIR with aboslute path?
# TODO: change WORKDIR to /app and see if that is equivalent,
# bc the Dockerfile is already in the repo
# and would remove the need for other users to manually edit 
# Dockerfile before running workflow.
# Keep in mind WORKDIR can use environment variables previously set
# using ENV, like ENV DIRPATH=/path followed by WORKDIR $DIRPATH/$DIRNAME
# without specifying a WORKDIR, default is /
WORKDIR /home/jcohen/viz-workflow/docker-parsl_workflow/app

COPY parsl_workflow.py .
COPY parsl_config.py .
# COPY viz_config.json .
COPY requirements.txt .

# import 1 UTM zone of lake change data to process in parallel until can access a persistent data volume on Datateam
COPY lake_change_cleaned.gpkg .

RUN pip install -r requirements.txt

# maybe we don't want to run a command bc we need to use the terminal to
# do it now that we are using parsl and k8s
# CMD [ "python", "./parsl_workflow.py" ]


# ------------------------------------------------------------

# # for simple_workflow.py:

# # base image
# FROM python:3.9

# WORKDIR /home/jcohen/viz-worflow/docker-parsl_workflow/

# # python script to run
# ADD simple_workflow.py .
# # add the input data
# COPY data/test_polygons.gpkg .
# COPY requirements.txt .

# # packages to install
# RUN pip install -r requirements.txt

# CMD [ "python", "./simple_workflow.py" ]




