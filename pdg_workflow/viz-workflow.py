# Example script for executing parsl workflow apps on kubernetes

import datetime
import parsl
from parsl import python_app

def run_pdg_workflow():
    '''Main workflow to execute all stats.'''

    parsl.set_stream_logger()
    #from parslexec import local_exec
    from pdg_workflow.parslexec import htex_kube
    #parsl.load(local_exec)
    parsl.load(htex_kube)

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
    run_pdg_workflow()

