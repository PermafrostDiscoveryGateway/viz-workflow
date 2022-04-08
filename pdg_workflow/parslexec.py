from parsl.config import Config
from parsl.channels import LocalChannel
from parsl.executors import HighThroughputExecutor
from parsl.executors.threads import ThreadPoolExecutor
from parsl.providers import LocalProvider
from parsl.providers import KubernetesProvider
from parsl.addresses import address_by_route

htex_local = Config(
    executors=[
        HighThroughputExecutor(
            label="htex_local",
            worker_debug=True,
            cores_per_worker=1,
            provider=LocalProvider(
                channel=LocalChannel(),
                init_blocks=1,
                max_blocks=1
            ),
        )
    ],
)

local_exec = Config(
    executors=[ 
        ThreadPoolExecutor( max_threads=10, label='local_exec') 
    ]
)

htex_kube = Config(
    executors=[
        HighThroughputExecutor(
            label='kube-htex',
            cores_per_worker=1,
            max_workers=2,
            worker_logdir_root='/',
            # Address for the pod worker to connect back
            #address=address_by_route(),
            address='192.168.0.103',
            #address_probe_timeout=3600,
            worker_debug=True,
            provider=KubernetesProvider(
                namespace="test",

                # Docker image url to use for pods
                image='mbjones/python3-parsl:0.2',

                # Command to be run upon pod start, such as:
                # 'module load Anaconda; source activate parsl_env'.
                # or 'pip install parsl'
                #worker_init='echo "Worker started..."; lf=`find . -name \'manager.log\'` tail -n+1 -f ${lf}',
                worker_init='echo "Worker started..."',

                # The secret key to download the image
                #secret="YOUR_KUBE_SECRET",

                # Should follow the Kubernetes naming rules
                pod_name='parsl-worker',

                nodes_per_block=1,
                init_blocks=2,
                min_blocks=1,
               # Maximum number of pods to scale up
                max_blocks=4,
            ),
        ),
    ]
)
