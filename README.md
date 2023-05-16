# PDG Ray Workflow

Permafrost Discovery Gateway visualization workflow that uses [viz-staging](https://github.com/PermafrostDiscoveryGateway/viz-staging), [viz-raster](https://github.com/PermafrostDiscoveryGateway/viz-raster/tree/main), and [viz-3dtiles](https://github.com/PermafrostDiscoveryGateway/viz-3dtiles) in parallel using Ray Core and Ray workflows.

## Running the Workflow

For release 0.9.0, scripts must be executed in a particular order for staging, rasterization, and web-tiling steps. 3d-tiling has not been tested for this release.

- ssh into the Delta server

-  Pull updates from the `main` branch for each of the 4 PDG repositories:
    - [`PermafrostDiscoveryGateway/viz-workflow`](https://github.com/PermafrostDiscoveryGateway/viz-workflow/tree/main)
    - [`PermafrostDiscoveryGateway/viz-staging`](https://github.com/PermafrostDiscoveryGateway/viz-staging)
    - [`PermafrostDiscoveryGateway/viz-raster`](https://github.com/PermafrostDiscoveryGateway/viz-raster)
    - [`PermafrostDiscoveryGateway/viz-3dtiles`](https://github.com/PermafrostDiscoveryGateway/viz-3dtiles) (Note: that release 0.9.0, 3D-tiling has not been fully implemented)

- Create a virtual environment with `python=3.9` and install __local__ versions of the PDG packages (using `pip install -e {LOCAL PACKAGE}`). Also install the packages:
    - `ray`
    - `glances`
    - `pyfastcopy`

- Prepare one of the two `slurm` scripts to claim some worker nodes on which we will launch a job.
    - Open the appropriate script that will soon be run to claim the nodes: either `viz-workflow/slurm/BEST-v2_gpu_ray.slurm` if you're using GPU, or `BEST_cpu_ray_double_srun.slurm` for CPU.
    - **Change the line `#SBATCH --nodes={NUMBER}`** which represents the number of nodes that will process the IWP data. 
    - **Change the line `#SBATCH --time=24:00:00`** which represents the total amount of time a job is allowed to run (and charge credits based on minutes and cores) before it is cancelled. The full 24 hours should be set if doing a full IWP run. 
    - **Change the line `#SBATCH --account={ACCOUNT NAME}`** and enter the account name for the appropriate allocation. Note that we do __not__ store these private account names on GitHub, so pay attention to this line when you are pushing. 
    - **Find the `# global settings section` and change `conda activate {ENVIRONMENT}` or `source path/to/{ENVIRONMENT}/bin/activate`** by entering your virtual environment for this workflow.

- Open a new terminal, start a `tmux` session, then activate your virtual environment. 

- Switch into the slurm dir by running `cd viz-workflow/slurm`. Then run `sbatch BEST-v2_gpu_ray.slurm` or `sbatch BEST_cpu_ray_double_srun.slurm` to launch the job on the number of nodes you specified within that file.

- Sync the most recent footprints from `/scratch` to all relevant nodes' `/tmp` dirs on Delta. Within a `tmux` session, switch into the correct environment, and ssh into the head node (e.g. `ssh gpub059`) and run `python viz-workflow/rsync_footprints_to_nodes.py`.

- Adjust `viz-workflow/PRODUCTION_IWP_CONFIG.py` as needed:
    - Change the variable `head_node` to the head node.
    - Specify the `INPUT` path and `FOOTPRINTS_REMOTE` paths.
    - Specify the `OUTPUT` path (which serves as the _start_ of the path for all output files). This includes a variable `output_subdir` which should be changed to something unique, such as any subfolders the user wants the data to be written to. Create this folder manually.
    - Within the subdir just created, created a subdirectory called `staged`.

- Adjust `viz-workflow/IN_PROGRESS_VIZ_WORKFLOW.py` as needed:
    - Within the `try:` part of the first function `main()`, comment out / uncomment out steps depending on your stage in the workflow. `step0_staging()` is first, so only uncomment this step.

-  Within the `tmux` session with your virtual environment activated, ssh into the head node associated with that job by running `ssh gpub059` or `ssh cn059`, for example. Then run: `python viz-workflow/IN_PROGRESS_VIZ_WORKFLOW.py`.

-  Once staging is complete, run `python viz-workflow/rsync_staging_to_scratch.py`.

-  Adjust the file `merge_staged_vector_tiles.py` as needed:
    - Set the variables `staged_dir_path_list` and `merged_dir_path`:
      - Change the last string part of `merged_dir_path` (where merged staged files will live) to the lowest number node of the nodes you're using (the head node).
      - Change the hard-coded nodes specified in `staged_dir_paths_list` to the list of nodes you're using for this job, except **do not include the head node in this list** because it was already assigned to `merged_dir_path`.
    - Within a `tmux` session, with your virtual environment activated, and ssh'd into the head node, run `python viz-workflow/merge_staged_vector_tiles.py`.

- Pre-populate your `/scratch` with a `geotiff` dir.

-  Return to the file `viz-workflow/IN_PROGRESS_VIZ_WORKFLOW.py` and comment out `step0_staging()`, and uncomment out the next step: `step2_raster_highest(batch_size=100)` (skipping 3d-tiling for release 0.9.0). Run `python viz-workflow/IN_PROGRESS_VIZ_WORKFLOW.py` in a `tmux` session with the virtual environment activated and ssh'd into the head node.

- Run `python viz-workflow/rsync_step2_raster_highest_to_scratch.py`.

-  Return to the file `viz-workflow/IN_PROGRESS_VIZ_WORKFLOW.py` and comment out `step2_raster_highest(batch_size=100)`, and uncomment out the next step: `step3_raster_lower(batch_size_geotiffs=100)`. Run `python viz-workflow/IN_PROGRESS_VIZ_WORKFLOW.py`.

- Check that the file `rasters_summary.csv` was written to `/scratch`. Download this file locally. If the top few lines look oddly fomatted, delete these lines and re-upload the file to the same directory (overwriting the misformatted one there).

- Create a new directory called `web_tiles` in your `/scratch` dir.

-  Return to `IN_PROGRESS_VIZ_WORKFLOW.py` and comment out the step: `step4_webtiles(batch_size_web_tiles=250)`. Run `python viz-workflow/IN_PROGRESS_VIZ_WORKFLOW.py`.

- Transfer the `log.log` in each nodes' `/tmp` dir to that respective nodes' subdir within `staging` dir: run `python viz-workflow/rsync_log_to_scratch.py`

- Cancel the job: `scancel {JOB ID}`. The job ID can be found on the left column of the output from `squeue | grep {USERNAME}`. No more credits are being used. Recall that the job will automatically be cancelled after 24 hours even if this command is not run.

- Remember to remove the `{ACCOUNT NAME}` for the allocation in the slurm script before pushing to GitHub.

- Move output data from Delta to the Arctic Data Center as soon as possible.

### Port Forward Ray Dashboard

1. Login to a login node on delta server

```
ssh <👉YOUR_NCSA_USERNAME👈>@login.delta.ncsa.illinois.edu
```
2. Start a Slurm job. `cpus-per-task` must be large (128 maximum on Delta) for Ray to scale well. 

```
# max CPU node request (for single node)
srun --account=<👉YOUR_CPU_ACCOUNT👈> --partition=cpu \
--nodes=1 --tasks=1 --tasks-per-node=1 \
--cpus-per-task=128 --mem=240g \
--time=02:00:00 \
--pty bash
```

3. Then SSH into the compute node you have running Ray. 

```
ssh cn001 (for example)
```
Forward port from compute node to your local personal computer:
```
ssh -L 8265:localhost:8265 <local_username>@<your_locaL_machine> 

# Navigate your web browser to: localhost:8265/
```

### Contributing

Please contribute via pull requests.

Documenting an environment can be done as follows:

```
conda env export | grep -v "^prefix: " > environment.yml
```
