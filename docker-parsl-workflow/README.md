# Run the visualization workflow with Docker

This directory contains all necessary files, except for input data, needed to execute the Permafrost Discovery Gateway visualization workflow with Docker, Kubernetes, and parsl for parallelization.

This repository includes 2 scripts:
  1. `simple_workflow.py` - a simple version of the visualization workflow with no parallelization
  2. `parsl_workflow.py` - more complex workflow that integrates parallelization with parsl and kubernetes

## Mounting a volume

These scripts can be run locally on a laptop, _or_ on a server. Either way, you will have to mount a persistent volume because both scripts write  output:
 - GeoPackage files in a `staging` directory
 - geoTIFF files in a `geotiff` directory 
 - web tiles in a `web_tiles` directory
 - supplementary `csv` files

See the documentation in the [`viz-staging`](https://github.com/PermafrostDiscoveryGateway/viz-staging) and [`viz-raster`](https://github.com/PermafrostDiscoveryGateway/viz-raster/tree/main) repositories for further information about the output. 
 
The difference in _how_ you mount a persistent volume is different between the simple workflow to the parallel workflow. In the parallel workflow, the `parsl` config ingests the filepath for the persistent volume, instead of specifying it in the `docker run` command.

## Python environment

Before executing either of these scripts, create a fresh environment with `conda` or `venv` and install all the requirements specified in the `requirements.txt` file with `pip install -r requirements.txt`

## 1. Run `simple_workflow.py`: Build an image and run the container

### Overview

- Execute the `docker build` command in the terminal, then the `docker run` command.
- When the script is complete, see the files written to a new output dir `app` in the `viz-workflow/docker-parsl/workflow` dir.
- Mounting a volume for output on a server is done in the same way as we do on our local laptop: we specify the filepath when we execute the `docker run` command! It's just a different filepath because now we are on a different machine.

### Steps 

- If working on a laptop rather than a server, clone repository & open the Docker Desktop app, then navigate to repository in VScode. If working on a server, SSH into the server in VScode, clone the repository, and navigate to the repository.
- Retrieve input data to process. **(TODO: make sample data accessible)**
- Edit the filepath to the input data in the Dockerfile as needed.
- Ensure an environment is activated in the terminal that is built from the same `requirements.txt` file as the docker image. This requires you to create a fresh environment, activate it, then run `pip install -r requirements.txt` in the command line. 
- Run `docker build -t image_name .` in the command line.
- Run the container and specify a persistent directory for input and output data, updating the path as needed: `docker run -v /path/to/repository/viz-workflow/docker-parsl-workflow/app:/app image_name`
    - Note: The `app` dir does not need ro be created manually, it will be created when the container runs.

## 2. Run `parsl_workflow.py`: Build an image and publish it to the repository, then run a container from the published repository package

### Overview

- This script runs the same visualization worklow but process in parallel with several workers. The amount of workers can be adjusted in `parsl_config.py`
- In the repository "packages" section there are published docker images that can be pulled by users. These packages are version controlled, so you can point to a specific image version to run. This makes a workflow more reproducibile.

### Steps

- SSH into server in VScode, clone the repository, and navigate to the repository.
- Make sure your Personal Access Token allows for publishing packages to the repository.
    - Navigate to your token on GitHub, then scroll down to `write:packages` and check the box and save.
- Edit the filepath to the input data in the Dockerfile as needed.
- Add a line to parsl config to specify the persistent volume name and mount filepath.
    - The first item in the list will need to be a persistent volume that is set up by the server admin.
    - The second argument is the location that you want the volume to be mounted within your container
```
persistent_volumes=persistent_volumes=[('pdgrun-dev-0', f'/home/{user}/viz-workflow/docker-parsl-workflow/app')]
```
- Update the string that represents the desired published repository package version of image in `parsl_config.py`. Replace the version number with the next version number you will publish it as:
```
image='ghcr.io/PermafrostDiscoveryGateway/viz-workflow:0.2',
```
- Publish the package to the repository with new version number by running 3 commands one-by-one. Replace `$GITHUB_PAT` with your PAT, and `{username}` with your github username:
```
docker build -t ghcr.io/julietcohen/docker_python_basics:0.9 .

echo $GITHUB_PAT | docker login ghcr.io -u {username} --password-stdin

docker push PermafrostDiscoveryGateway/viz-workflow:0.2
```
- Run `kubectl get pods` to see if any pods are left hanging from the last run. This could be the case if a past run failed to shut down the parsl workers.
    - If there are any hanging, delete them all at once for the specific namespace by running `kubectl delete pods --all -n {namespace}`
- Ensure an environment is activated in the terminal that is build from the same `requirements.txt` file as the docker image. 
- Run the python script for the parsl workflow: `python parsl_workflow.py`

**General Notes:**
- If the run is successful, parsl processes should shut down cleanly. If not, you'll need to kill the processes manually.
  - You can check your processes in the command line with `ps -ef | grep {username}`
  - In the output, the column next to your username shows the 5-digit identifier for the proess. Run `kill -9 {identifier}` to kill one in particular.
- After each run, if files were output, remove them from the persistent directory before next run. 