# for parsl_workflow.py:

FROM python:3.9

# metadata info:
LABEL org.opencontainers.image.source https://github.com/permafrostdiscoverygateway/viz-workflow

# note: is it necessary to start WORKDIR with aboslute path?
# could change WORKDIR to /app and see if that is equivalent,
# bc the Dockerfile is already in the repo
# and would remove the need for other users to manually edit 
# Dockerfile before running workflow.
# Keep in mind WORKDIR can use environment variables previously set
# using ENV, like ENV DIRPATH=/path followed by WORKDIR $DIRPATH/$DIRNAME
# without specifying a WORKDIR, default is /

# use a generalized WORKDIR per suggestion from dev team:
# PROBLEM: CANNOT CREATE app dir in /usr/local/share/ BECAUSE DO NOT HAVE PERMISSION 
# WORKDIR /usr/local/share/app
WORKDIR /home/jcohen/viz-workflow/docker-parsl_workflow/app 
# check that `app` subdir is created auomatically
# should app be included here? best practice may be to not use the app dir as WORKDIR
# but since we might be tranferring a lot of data as input to workflow,
# maybe using a mounted volume here is best to store both input and output

COPY parsl_workflow.py .
COPY parsl_config.py .
# COPY viz_config.json . # pasted config into parsl_workflow.py
COPY requirements.txt .

# import 2 adjacent UTM zones of lake change data to process in parallel 
COPY iwp_2_files .

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




