FROM python:3.11-slim

# Install transitive dependencies and build tools
RUN apt-get update \
    && apt-get install -y git libspatialindex-dev libgdal-dev libproj-dev build-essential g++ libegl1 libgl1 libgomp1 libusb-1.0-0 \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN pip install uv

# Install pdgworkflow from GitHub repo using uv
# RUN uv pip install --system git+https://github.com/PermafrostDiscoveryGateway/viz-workflow.git@main

WORKDIR /app
COPY . /app
RUN uv pip install --system .

ENV TILEMATRIXSET_DIRECTORY=/app/custom_tms

CMD ["python3"]
