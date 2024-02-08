# # for parsl_workflow.py:

# FROM python:3.9

# LABEL org.opencontainers.image.source https://github.com/permafrostdiscoverygateway/viz-workflow

# WORKDIR /home/jcohen/viz-worflow/docker-parsl_workflow/

# COPY parsl_workflow.py .
# COPY parsl_config.py .
# COPY viz_config.json .
# COPY requirements.txt .

# # import 1 UTM zone of lake change data to process in parallel until can access a persistent data volume on Datateam
# COPY lake_change_cleaned.gpkg .

# RUN pip install -r requirements.txt

# # maybe we don't want to run a command bc we need to use the terminal to
# # do it now that we are using parsl and k8s
# # CMD [ "python", "./parsl_workflow.py" ]


# ------------------------------------------------------------

# for simple_workflow.py:

# base image
FROM python:3.9

WORKDIR /home/jcohen/viz-worflow/docker-parsl_workflow/

# python script to run
ADD simple_workflow.py .
# add the input data
COPY data/test_polygons.gpkg .
COPY requirements.txt .

# packages to install
RUN pip install -r requirements.txt

CMD [ "python", "./simple_workflow.py" ]




