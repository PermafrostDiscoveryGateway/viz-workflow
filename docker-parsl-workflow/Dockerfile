# for parsl_workflow.py:

FROM python:3.9

# metadata info:
LABEL org.opencontainers.image.source https://github.com/permafrostdiscoverygateway/viz-workflow

# Keep in mind WORKDIR can use environment variables previously set
# using ENV, like ENV DIRPATH=/path followed by WORKDIR $DIRPATH/$DIRNAME

# WORKDIR /usr/local/share/app # a generalized option
WORKDIR /app 
# check that `app` subdir is created automatically

# Create new group called pdgk8sgroup and add new user to that group
# both with same ID number as jcohen for permissions after container runs
RUN groupadd --gid 1040 -r pdgk8sgroup && useradd --uid 1040 -r -g pdgk8sgroup pdgk8suser
# actviate that user account
USER pdgk8sgroup:pdgk8suser

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY workflow_config.py .
COPY iwp_2_files .

COPY parsl_workflow.py .
COPY parsl_config.py .

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




