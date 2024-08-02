from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.providers import KubernetesProvider
from parsl.addresses import address_by_route

def config_parsl_cluster(
        max_blocks = 4,
        min_blocks = 1,
        init_blocks = 1,
        max_workers = 4,
        cores_per_worker = 1,
        # TODO: automate this following string to pull most recent release on github?
        image='ghcr.io/permafrostdiscoverygateway/viz-workflow:0.3.1',
        namespace='viz-workflow'):

    htex_kube = Config(
        executors = [
            HighThroughputExecutor(
                label = 'kube-htex',
                cores_per_worker = cores_per_worker,
                max_workers = max_workers,
                worker_logdir_root = '/',
                # Address for the pod worker to connect back
                address = address_by_route(),
                worker_debug = True,
                # increased seconds allocated towards a task before it is 
                # considered failed,for loading in large files:
                heartbeat_threshold = 3600,
                # default seconds for how often a heartbeat is sent:
                heartbeat_period = 30, 
                provider = KubernetesProvider(

                    # Namespace in K8S to use for the run
                    namespace = namespace,

                    # Docker image url to use for pods
                    image = image,

                    # Should follow the Kubernetes naming rules
                    pod_name = 'viz-workflow-worker',

                    init_mem='4Gi',
                    max_mem='8Gi',

                    nodes_per_block = 1,
                    init_blocks = init_blocks,
                    min_blocks = min_blocks,
                    # Maximum number of pods to scale up
                    max_blocks = max_blocks,
                    # persistent_volumes (list[(str, str)]) – List of tuples
                    # describing persistent volumes to be mounted in the pod.
                    # The tuples consist of (PVC Name, Mount Directory).
                    persistent_volumes = [('viz-workflow-pvc', f'/data')],

                    # This annotation is required to mount a GCS PVC to the pod and
                    # the service account (with sufficient permissions) is required
                    # to access a GCS PVC.
                    annotations={"gke-gcsfuse/volumes": "true"},
                    service_account_name="viz-workflow-sa",
                ),
            ),
        ]
    )
    return(htex_kube)
