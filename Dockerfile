FROM python:3.9-slim

# Install transitive dependencies
RUN apt-get update && apt-get install -y \
    git \
    libspatialindex-dev \
    libgdal-dev \
    libproj-dev \
    g++ \
    gcc \
    python3-dev \
    build-essential \
    pkg-config \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install pdgstaging from GitHub repo
RUN pip install --upgrade pip
RUN pip install git+https://github.com/PermafrostDiscoveryGateway/viz-workflow.git@feature-wf-k8s

WORKDIR /app

CMD ["python3"]
