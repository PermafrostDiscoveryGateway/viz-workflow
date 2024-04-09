# for parsl_workflow.py:

#FROM python:3.9
#
## metadata info:
#LABEL org.opencontainers.image.source https://github.com/permafrostdiscoverygateway/viz-workflow
#
## Keep in mind WORKDIR can use environment variables previously set
## using ENV, like ENV DIRPATH=/path followed by WORKDIR $DIRPATH/$DIRNAME
#
## WORKDIR /usr/local/share/app # a generalized option
#WORKDIR /app
## check that `app` subdir is created automatically
#
#COPY requirements.txt .
#RUN pip install -r requirements.txt
#COPY workflow_config.py .
#
## import 2 adjacent UTM zones of lake change data to process in parallel
#COPY iwp_2_files .
#
#COPY parsl_workflow.py .
#COPY parsl_config.py .

# maybe we don't want to run a command bc we need to use the terminal to
# do it now that we are using parsl and k8s
# CMD [ "python", "./parsl_workflow.py" ]


# ------------------------------------------------------------

# # for simple_workflow.py:

# # base image
FROM python:3.9

WORKDIR /Users/helium/pdg/viz-worflow/docker-parsl_workflow/

 # python script to run
ADD simple_workflow.py .
 # add the input data
COPY data/test_polygons.gpkg .
COPY requirements.txt .

 # packages to install
RUN pip install -r requirements.txt

CMD [ "python", "./simple_workflow.py" ]




