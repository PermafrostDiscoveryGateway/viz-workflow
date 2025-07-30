# Viz-workflow: the Permafrost Discovery Gateway geospatial data visualization workflow

- **Authors**: Robyn Thiessen-Bock ; Juliet Cohen ; Matthew B. Jones ; Kastan Day ; Lauren Walker
- **DOI**: [10.18739/A2599Z362](https://ezid.cdlib.org/id/doi:10.18739/A2599Z362)
- **License**: [Apache 2](https://opensource.org/license/apache-2-0/)
- [Package source code on GitHub](https://github.com/PermafrostDiscoveryGateway/viz-workflow)
- [Submit bugs and feature requests](https://github.com/PermafrostDiscoveryGateway/viz-workflow/issues/new)

The Permafrost Discovery Gateway visualization workflow uses [viz-staging](https://github.com/PermafrostDiscoveryGateway/viz-staging), [viz-raster](https://github.com/PermafrostDiscoveryGateway/viz-raster/tree/main), and [viz-3dtiles](https://github.com/PermafrostDiscoveryGateway/viz-3dtiles) in parallel using Ray Core and Ray workflows. An alternative workflow that uses `Docker` and `parsl` for parallelization is currently under development.

![PDG workflow summary](docs/images/viz_workflow.png)

## Citation

Cite this software as:

> Robyn Thiessen-Bock, Juliet Cohen, Matthew B. Jones, Kastan Day, Lauren Walker. 2023. Viz-workflow: the Permafrost Discovery Gateway geospatial data visualization workflow (version 1.0.0). Arctic Data Center. doi: 10.18739/A2599Z362

## Install

Requires Python version `3.9` and `libspatialindex` or `libspatialindex-dev`

### Prerequisites

1. Follow the instructions to install [`libspatialindex`](https://libspatialindex.org/en/latest/) or [`libspatialindex-dev`](https://packages.ubuntu.com/bionic/libspatialindex-dev)
2. Make sure that Python version 3.9 is installed (try `which python3.9`).

### Installation Options

#### Option 1: Using uv (recommended)

Install `pdgworkflow` from GitHub repo using uv:

```bash
# Install the package
uv pip install git+https://github.com/PermafrostDiscoveryGateway/viz-workflow.git

# Or install with development dependencies
uv pip install "git+https://github.com/PermafrostDiscoveryGateway/viz-workflow.git[dev]"
```

#### Option 2: Using pip

Install `pdgworkflow` from GitHub repo using pip:

```bash
# Install the package
pip install git+https://github.com/PermafrostDiscoveryGateway/viz-workflow.git

# Or install with development dependencies
pip install "git+https://github.com/PermafrostDiscoveryGateway/viz-workflow.git[dev]"
```

#### Option 3: Using a virtual environment

If you prefer to use a virtual environment:

```bash
# Create and activate a virtual environment
python3.9 -m venv venv
source venv/bin/activate  # On macOS/Linux
# or
venv\Scripts\activate     # On Windows

# Install with uv
uv pip install git+https://github.com/PermafrostDiscoveryGateway/viz-workflow.git

# Or install with pip
pip install git+https://github.com/PermafrostDiscoveryGateway/viz-workflow.git

# To deactivate the virtual environment when done
deactivate
```

#### For Development

If you're developing this package, clone the repository and install in editable mode:

```bash
# Clone the repository
git clone https://github.com/PermafrostDiscoveryGateway/viz-workflow.git
cd viz-workflow

# Option 1: Using uv to create and manage virtual environment
uv venv
source .venv/bin/activate
uv pip install -e ".[dev]"

# Option 2: Using uv with custom virtual environment
python3.9 -m venv venv
source venv/bin/activate
uv pip install -e ".[dev]"

# Option 3: Using pip with virtual environment
python3.9 -m venv venv
source venv/bin/activate
pip install -e ".[dev]"

# Option 4: Direct installation with uv (creates managed environment)
uv pip install -e ".[dev]"

# Option 5: Direct installation with pip
pip install -e ".[dev]"
```

The development dependencies include:
- `pytest >= 7` - for running tests
- `black >= 24.1.1` - for code formatting
- `pre-commit >= 4.1` - for pre-commit hooks

## Usage

To run the visualization workflow with Ray on the National Center for Supercomputing Applications Delta server, see documentation in [`PermafrostDiscoveryGateway/viz-info/09_iwp-workflow`](https://github.com/PermafrostDiscoveryGateway/viz-info/blob/main/09_iwp-workflow.md)

**From the command line:**
- run: `python -m pdgworkflow -c '/path/to/config.json'`

**In python:**

```python
import pdgworkflow
# See example usage in documentation
```

See more example code in [`PermafrostDiscoveryGateway/viz-info/helpful-code`](https://github.com/PermafrostDiscoveryGateway/viz-info/tree/main/helpful-code)

### Port Forward Ray Dashboard

1. Login to a login node on Delta server

```bash
ssh <👉YOUR_NCSA_USERNAME👈>@login.delta.ncsa.illinois.edu
```

2. Start a Slurm job. `cpus-per-task` must be large (128 maximum on Delta) for Ray to scale well. 

```bash
# max CPU node request (for single node)
srun --account=<👉YOUR_CPU_ACCOUNT👈> --partition=cpu \
--nodes=1 --tasks=1 --tasks-per-node=1 \
--cpus-per-task=128 --mem=240g \
--time=02:00:00 \
--pty bash
```

3. Then SSH into the compute node you have running Ray. 

```bash
ssh cn001 (for example)
```

Forward port from compute node to your local personal computer:

```bash
ssh -L 8265:localhost:8265 <local_username>@<your_locaL_machine> 

# Navigate your web browser to: localhost:8265/
```

## PDG Visualization Workflow

This repository contains the orchestration code that coordinates the entire Permafrost Discovery Gateway (PDG) visualization pipeline. The workflow integrates three main processing steps:

1. **Staging** - Uses [viz-staging](https://github.com/PermafrostDiscoveryGateway/viz-staging) to prepare vector data by dividing it into tiled vector files
2. **Rasterization** - Uses [viz-raster](https://github.com/PermafrostDiscoveryGateway/viz-raster) to convert staged vector data into raster tiles and images
3. **3D Tiles** - Uses [viz-3dtiles](https://github.com/PermafrostDiscoveryGateway/viz-3dtiles) to create 3D visualization tiles

The workflow uses Ray Core and Ray workflows for parallel processing, enabling efficient processing of large geospatial datasets across multiple compute nodes. An alternative workflow using Docker and Parsl for parallelization is under development.

## Development

Build and test using standard Python tools.

- To test, run `pytest` from the root of the package directory
- To format code, run `black .`
- VS Code configuration is setup to configure tests as well

## License

```
Copyright [2013] [Regents of the University of California]

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
