"""Main module."""

import datetime
import parsl
from parsl import python_app
from parsl.config import Config
#from parsl.channels import LocalChannel
from parsl.executors import HighThroughputExecutor
#from parsl.executors.threads import ThreadPoolExecutor
#from parsl.providers import LocalProvider
from parsl.providers import KubernetesProvider
from parsl.addresses import address_by_route
from kubernetes import client, config

def init_parsl():
    parsl.set_stream_logger()
    #from parslexec import local_exec
    #from parslexec import htex_kube

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
    #parsl.load(local_exec)
    parsl.load(htex_kube)

def run_pdg_workflow():
    '''Main workflow to execute all stats.'''

    size = 5
    stat_results = []
    for year in range(size):
        for lat in range(size):
            current_time = datetime.datetime.now()
            print(f'Schedule job at {current_time} for {year} and {lat}')
            stat_results.append(calc_stat_lat(year, lat))
    stats = [r.result() for r in stat_results]
    print(sum(stats))

@python_app
def calc_stat_lat(year, lat):
    import datetime
    import time
    current_time = datetime.datetime.now()
    print(f'Starting job at {current_time} for {year} and {lat}')
    prod = year*lat
    time.sleep(5)
    return(prod)


if __name__ == "__main__":
    init_parsl()
    run_pdg_workflow()

