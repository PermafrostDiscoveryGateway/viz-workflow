# Run the visualization workflow with Docker

This directory contains everything necessary to execute the Permafrost Discovery Gateway visualization workflow with Docker, Kubernetes, and parsl for parallelization.

The following 2 scripts can be run within a Docker container:
  1. `simple_workflow.py` - a simple version of the visualization workflow with no parallelization
  2. `parsl_workflow.py` - more complex workflow that integrates parallelization with parsl and kubernetes

## Python environment

Before executing either of these scripts, create a fresh environment with `conda` or `venv` and install all the requirements specified in the `requirements.txt` file with `pip install -r requirements.txt`

## Persistent data volumes

These scripts can be run locally on a laptop, _or_ on a server. Either way, you will have to specify a persistent data volume because both scripts write the following output:
 - GeoPackage files to a `staging` directory
 - GeoTIFF files to a `geotiff` directory 
 - web tiles to a `web_tiles` directory
 - supplementary `csv` files output by the visualization workflow
 - a log file

See the documentation in the [`viz-staging`](https://github.com/PermafrostDiscoveryGateway/viz-staging) and [`viz-raster`](https://github.com/PermafrostDiscoveryGateway/viz-raster/tree/main) repositories for further information about the output. 
 
_How_ you specify a persistent data volume for the container differs between the simple workflow and the parallel workflow. In the non-parallelized workflow, we specify the path to the directory of choice within the `docker run` command. In the parallel workflow, the `parsl` config includes the filepath for the mounted persistent volume, and the volume must be configured beforehand. See details for these within the steps for each scipt below.

## 1. Run `simple_workflow.py`: Build an image and run the container

### Overview

- Execute the `docker build` command in the terminal, then the `docker run` command.
- When the script is complete, see the files written to a new output dir `app` in the `viz-workflow/docker-parsl/workflow` dir.
- Mounting a volume for output on a server is done in the same way as we do on our local laptop: we specify the filepath when we execute the `docker run` command! It's just a different filepath because now we are on a different machine.

### Steps 

- If working on a laptop rather than a server, clone repository & open the Docker Desktop app, then navigate to repository in VScode. If working on a server, SSH into the server in VScode, clone the repository, and navigate to the repository.
- Ensure an environment is activated in the terminal that is built from the same `requirements.txt` file as the docker image. This requires you to create a fresh environment, activate it, then run `pip install -r requirements.txt` in the command line. 
- Retrieve input data to process.
  - **(TODO: make sample data accessible to people without access to Datateam)**
- Edit the filepath for the WORKDIR in the Dockerfile as needed.
  - **TODO: automate this step by adjusting `WORKDIR` in Dockerfile, and potentially defining `ENV` variables prior to defining the `WORKDIR` that are then used in the `WORKDIR`.**
  - Recall that the default `WORKDIR` is `/` if not otherwise defined.
- Run `docker build -t image_name .` in the command line.
- Run the container and specify a persistent directory for input and output data, updating the path as needed: `docker run -v /path/to/repository/viz-workflow/docker-parsl-workflow/app:/app image_name`
    - Note: The `app` dir does not need ro be created manually, it will be created when the container runs.

## 2. Run `parsl_workflow.py`: Build an image and publish it to the repository, then run a container from the published repository package

### Overview

- This script runs the same visualization worklow but processes in parallel with several workers. The amount of workers can be adjusted in the configuration: `parsl_config.py`
- The GitHub repository "packages" section contains all published Docker images that can be pulled by users. These  are version controlled, so you can point to a specific image version to run. This makes a workflow more reproducibile. The repo and version are specified in the `parsl_config.py`

### Steps

- Make sure your GitHub Personal Access Token allows for publishing packages to the repository.
    - Navigate to your token on GitHub, then scroll down to `write:packages` and check the box and save.
- SSH into server in VScode, clone the repository, and navigate to the repository.
- Ensure an environment is activated in the terminal that is built from the same `requirements.txt` file as the docker image, and the same version of python.
- Edit the line in the parsl configuration to specify the persistent volume name and mount filepath.
    - The first item in the list will need to be a persistent volume that is set up by the server admin. See [this repository](https://github.com/mbjones/k8s-parsl?tab=readme-ov-file#persistent-data-volumes) for details.
    - The second item is the location that you want the volume to be mounted _within your container_. `/mnt/data` is a common path used in this field. Recall that the data will be written there in the container but will be persistently accessible at the location of the persistent volume on your machine.
```
persistent_volumes=persistent_volumes=[('pdgrun-dev-0', f'/mnt/data')]
```
- Update the string that represents the desired published repository package version of image in `parsl_config.py`. Replace the version number with the next version number you will publish it as:
```
image='ghcr.io/permafrostdiscoverygateway/viz-workflow:0.1.2',
```
- Publish the package to the repository with new version number by running 3 commands one-by-one:
```
docker build -t ghcr.io/permafrostdiscoverygateway/viz-workflow:0.1.2 .
```
Note: The string representing the organization and repo in these commands must be all lower-case.

```
echo $GITHUB_TOKEN | docker login ghcr.io -u $GITHUB_USER --password-stdin
```
```
docker push ghcr.io/permafrostdiscoverygateway/viz-workflow:0.1.2
```
- Run `kubectl get pods` to see if any pods are left hanging from the last run in your namespace. This could be the case if a past run failed to shut down the parsl workers.
    - If there are any hanging, delete them all at once for the specific namespace by running: `kubectl delete pods --all -n {namespace}`
    - or take the safer route by deleting them by listing each pod name: `kubectl delete pods {podname} {podname} {podname}`
- Run the python script for the parsl workflow, specifying to print the log output to file: `python parsl_workflow.py > k8s_parsl.log 2>&1 `

**General Notes:**
- If the run is successful, parsl processes should shut down cleanly. If not, you'll need to kill the processes manually.
  - You can check your processes in the command line with `ps -ef | grep {username}`
  - In the output, the column next to your username shows the 5-digit identifier for the proess. Run `kill -9 {identifier}` to kill one in particular.
- After each run, if files were output, remove them from the persistent directory before next run. 
- If testing code and you end up building many images, run `docker images` to list them and you can choose which to delete