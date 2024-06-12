from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.providers import KubernetesProvider
from parsl.addresses import address_by_route

# not necessary if mounting volume at /usr/local/share/app:
# import subprocess
# user = subprocess.check_output("whoami").strip().decode("ascii")

def config_parsl_cluster(
        max_blocks = 4, 
        min_blocks = 1, 
        init_blocks = 1, 
        max_workers = 4, 
        cores_per_worker = 1, 
        # TODO: automate this following string to pull most recent release on github? 
        image='ghcr.io/permafrostdiscoverygateway/viz-workflow:0.2.6',
        namespace='pdgrun'):

    htex_kube = Config(
        executors = [
            HighThroughputExecutor(
                label = 'kube-htex',
                cores_per_worker = cores_per_worker,
                max_workers = max_workers,
                worker_logdir_root = '/',
                # Address for the pod worker to connect back
                address = address_by_route(),
                # address='128.111.85.174',
                #address_probe_timeout=3600,
                worker_debug = True,
                provider = KubernetesProvider(
    
                    # Namespace in K8S to use for the run
                    namespace = namespace,
                    
                    # Docker image url to use for pods
                    image = image,
    
                    # Command to be run upon pod start, such as:
                    # 'module load Anaconda; source activate parsl_env'.
                    # or 'pip install parsl'
                    # worker_init='echo "Worker started..."; lf=`find . -name \'manager.log\'` tail -n+1 -f ${lf}',
                    worker_init = 'pip install parsl==2023.11.27',
    
                    # Should follow the Kubernetes naming rules
                    pod_name = 'parsl-worker',
    
                    nodes_per_block = 1,
                    init_blocks = init_blocks,
                    min_blocks = min_blocks,
                    # Maximum number of pods to scale up
                    max_blocks = max_blocks,
                    # persistent_volumes (list[(str, str)]) – List of tuples 
                    # describing persistent volumes to be mounted in the pod. 
                    # The tuples consist of (PVC Name, Mount Directory).
                    persistent_volumes = [('pdgrun-dev-0', f'/mnt/k8s-dev-pdg')]
                ),
            ),
        ]
    )
    return(htex_kube)