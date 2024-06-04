# for parsl_workflow.py:

# Note: the order of the steps may need to be adjusted

# FROM ubuntu:22.04 is also an otpion but this would make the image larger and would need to install python too
FROM python:3.9
SHELL ["/bin/bash", "-c"]
# metadata info:
LABEL org.opencontainers.image.source https://github.com/permafrostdiscoverygateway/viz-workflow

# WORKDIR /usr/local/share/app # a generalized option
# Keep in mind WORKDIR can use environment variables previously set
# using ENV, like ENV DIRPATH=/path followed by WORKDIR $DIRPATH/$DIRNAME
WORKDIR /home/pdgk8suser

RUN apt update && apt -y install wget sudo vim nano iproute2 tree
# pip should already be installed after installing python, so no need to install here

# Create new group called pdgk8sgroup and add new user to that group
# both with same ID number as jcohen for permissions after container runs.
# Do this before miniconda operations because want to install miniconda in the 
# user's homedir?
RUN groupadd --gid 1040 -r pdgk8sgroup && useradd --uid 1040 -r -g pdgk8sgroup pdgk8suser
# make dir that matches WORKDIR
RUN mkdir -p /home/pdgk8suser && chown pdgk8suser:pdgk8sgroup /home/pdgk8suser
# make dir that matches the PV to store output data
RUN mkdir -p /mnt/k8s-dev-pdg && chown pdgk8suser:pdgk8sgroup /mnt/k8s-dev-pdg

# actviate that user account
USER pdgk8suser:pdgk8sgroup

# define miniconda installation path based on WORKDIR
ENV CONDA_HOME="/home/pdgk8suser/miniconda3"
ENV PATH="${CONDA_HOME}/bin:${PATH}"

RUN mkdir -p ${CONDA_HOME} && \
    wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ${CONDA_HOME}/miniconda.sh && \
    bash ${CONDA_HOME}/miniconda.sh -b -u -p ${CONDA_HOME} && \
    rm -rf ${CONDA_HOME}/miniconda.sh && \
    conda --version

# create new conda env
RUN conda create -n pdg_k8s_env python=3.9

SHELL ["conda", "run", "-n", "pdg_k8s_env", "/bin/bash", "-c"]

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




