FROM python:3.9

LABEL org.opencontainers.image.source https://github.com/permafrostdiscoverygateway/viz-workflow

WORKDIR /usr/local/share/app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY parsl_workflow.py .
