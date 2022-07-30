#!/bin/bash -l

#SBATCH --job-name=pdg_viz
#SBATCH --partition=gpuA40x4
#SBATCH --account=bbki-delta-gpu
#SBATCH --time=24:00:00

#SBATCH --export=ALL,RAY_worker_register_timeout_seconds=120

#SBATCH --nodes=15
#SBATCH --mem=0
#SBATCH --gpus-per-node=1
#SBATCH --exclusive

set -x

## Set mem=0 to allocate entire memory block. Much safer.
## total any-node-mem is 252g
## total cpu-node cpus is 128  (dual socket)
## total gpu-node cpus is 64   (single socket)



## UPDATES ##
## 1. tasks-per-node used to be 1, but then I was only seeing 1 node of usage at a time.
## 2. I removed the --exclusive so it can run on multiple nodes at once.
## 3. Todo add #SBATCH --tasks=50... or something.


# one thread per node... or per task? I think set it for total number of CPU cores... (10x60)
# In multi-worker setting, we want one thread per worker instead of many threads per worker to avoid contention.
# https://docs.ray.io/en/latest/ray-core/configure.html#cluster-resources
# OMP_NUM_THREADS=600
# export OMP_NUM_THREADS

echo "This is BEST-v2_gpu_ray.slurm"

# global settings
conda activate full_viz_pipeline
export RAY_worker_register_timeout_seconds=120  # for sure necessary. Might be needed in user shell before `sbatch` is executed. 
export RAY_max_pending_lease_requests_per_scheduling_category=2000 # probably not necessary

# Getting the node names
nodes=$(scontrol show hostnames "$SLURM_JOB_NODELIST")
nodes_array=($nodes)
head_node=${nodes_array[0]}
head_node_ip=$(srun --nodes=1 --ntasks=1 -w "$head_node" hostname --ip-address)

# if we detect a space character in the head node IP, we'll
# convert it to an ipv4 address. This step is optional.
if [[ "$head_node_ip" == *" "* ]]; then
IFS=' ' read -ra ADDR <<<"$head_node_ip"
if [[ ${#ADDR[0]} -gt 16 ]]; then
  head_node_ip=${ADDR[1]}
else
  head_node_ip=${ADDR[0]}
fi
echo "IPV6 address detected. We split the IPV4 address as $head_node_ip"
fi

# Set Ray Head node IP
port=6379
ip_head=$head_node_ip:$port
export ip_head
echo "IP Head: $ip_head"

# START RAY HEAD
echo "Starting HEAD at $head_node"
srun --nodes=1 --ntasks=1 -w "$head_node" \
    ray stop
srun --nodes=1 --ntasks=1 -w "$head_node" \
    ray start --head \
    --node-ip-address="$head_node_ip" --port=$port \
    --dashboard-host 0.0.0.0 --log-color true --block &

# optional, though may be useful in certain versions of Ray < 1.0.
sleep 1

# START RAY WORKERS

# number of nodes other than the head node
worker_num=$((SLURM_JOB_NUM_NODES - 1))
for ((i = 1; i <= worker_num; i++)); do
    node_i=${nodes_array[$i]}
    srun --nodes=1 --ntasks=1 -w "$node_i" \
    ray stop
    echo "Starting WORKER $i at $node_i"
    srun --nodes=1 --ntasks=1 -w "$node_i" \
      ulimit -n unlimited
    srun --nodes=1 --ntasks=1 -w "$node_i" \
      export RAY_worker_register_timeout_seconds=120
    srun --nodes=1 --ntasks=1 -w "$node_i" \
        ray start --address "$ip_head" --log-color true --block &
    sleep 0.25
    echo "WORKER $i has SLURM_CPUS_PER_TASK: ${SLURM_CPUS_PER_TASK}"
done

sleep infinity # wait forever to presist the ray runtime
