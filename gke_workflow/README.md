# Run the visualization workflow on GKE

Note: This documentation is written to be self-contained, but see also the [PDG GCP infra documentation](https://github.com/PermafrostDiscoveryGateway/pdg-tech/blob/master/gcloud/gcloud-infrastructure.md#gcp-kubernetes-clusters), the [Ray cluster setup instructions](https://github.com/PermafrostDiscoveryGateway/pdg-tech/blob/master/gcloud/raycluster/README.md), and the [docker-parsl-workflow instructions](https://github.com/PermafrostDiscoveryGateway/viz-workflow/blob/enhancement-1-k8s/docker-parsl-workflow/README.md), which contain some similar information.

## Prerequisites

We’re using the following shared GKE autopilot cluster that’s already been set up in the PDG project:

* project: `pdg-project-406720`
* cluster: `pdg-autopilot-cluster-1`
* region: `us-west1`

Many of the following instructions use a CLI to deploy changes in the cluster. I’m using Cloud Shell to SSH into an existing GCE VM instance set up for this purpose and then running commands from there, since there are networking restrictions preventing us from running commands directly from Cloud Shell or many other places. (There may be other ways to set up your terminal as well.) To do this:

1. From Cloud Console VM instances page, start the existing GCE VM instance if it’s currently stopped
    * instance name: `pdg-gke-entrypoint`
    * zone: `us-west1-b`
2. SSH into the VM instance:
    ```
    $ gcloud compute ssh --zone us-west1-b pdg-gke-entrypoint --project pdg-project-406720
    ```
3. Set up authorization to the cluster:
    ```
    $ gcloud auth login
    $ gcloud container clusters get-credentials pdg-autopilot-cluster-1 --internal-ip --region us-west1 --project pdg-project-406720
    ```

## One-time setup

The following three objects have been set up once and don't need to be changed during a normal execution - see [Running the script](#running-the-script) below instead for what to change each time you want to rerun the script. However these setup steps are documented for reference in case the setup needs to be modified in the future. Note the service account and persistent volume/persistent volume claim are all namespace-scoped, so one of each needs to be created in every new namespace.

1. A namespace within the cluster
    * namespace name: `viz-workflow`
    * See [Set up namespace](#set-up-namespace) instructions below
2. A (Kubernetes) service account for accessing the GCS bucket
    * service account name: `viz-workflow-sa`
    * See [Set up service account](#set-up-service-account) instructions below
3. A persistent volume/claim pointing to the GCS bucket where we store input/output files
    * persistent volume name: `viz-workflow-pv`
    * persistent volume claim name: `viz-workflow-pvc`
    * See [Set up persistent volume](#set-up-persistent-volume) instructions below

### Set up namespace

Kubernetes namespaces provide logical separation between workloads in the cluster. Our cluster is shared between the viz workflow and the PDG data pipelines, so I created a namespace to separate the viz workflow:

1. Create the namespace:
    ```
    $ kubectl create namespace viz-workflow
    ```

### Set up service account

The service account needs to be set up for the leader and worker pods to have permissions to access the GCS bucket. However, because it’s what’s used within the leader pod, it also will need to have permissions on the GKE cluster to be able to modify the cluster to turn up worker pods. This generally follows the GCP docs to [configure KSAs](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity#configure-authz-principals), [configure GCS persistent volume auth](https://cloud.google.com/kubernetes-engine/docs/how-to/persistent-volumes/cloud-storage-fuse-csi-driver#authentication), and [configure RBAC permisssions](https://cloud.google.com/kubernetes-engine/docs/how-to/role-based-access-control).

1. Create the (Kubernetes) service account:
    ```
    $ kubectl create serviceaccount viz-workflow-sa --namespace viz-workflow
    ```

Ask someone with access to grant the IAM role with GCS permissions to the KSA:

2. Grant the Storage Object User IAM role to the KSA:
    ```
    $ gcloud projects add-iam-policy-binding pdg-project-406720 --member=principal://iam.googleapis.com/projects/896944613548/locations/global/workloadIdentityPools/pdg-project-406720.svc.id.goog/subject/ns/viz-workflow/sa/viz-workflow-sa --role=roles/storage.objectUser
    ```
    As an alternative, it should be possible to grant permissions only on a specific GCS bucket to the KSA if you prefer - see the GCP doc above.

According to the GCP docs, it should be possible to grant the GKE permissions to the KSA either through an IAM role or through Kubernetes RBAC permissions (which are more fine-grained than the IAM role) - however, I haven’t been able to get the IAM role option working. Instead you can just set up RBAC permissions:

3. Modify the manifests (if needed):
    * Example role: [manifests/service_account_role.yaml](manifests/service_account_role.yaml)
    * Example role binding: [manifests/service_account_role_binding.yaml](manifests/service_account_role_binding.yaml)
4. Create the RBAC role:
    ```
    $ kubectl apply -f service_account_role.yaml
    ```
5. Create the RBAC role binding:
    ```
    $ kubectl apply -f service_account_role_binding.yaml
    ```

### Set up persistent volume

Currently there’s only one GCS bucket that’s shared across all workflows, which contains a subdirectory for viz-workflow:

* bucket name: `pdg-storage-default`

In the future it’s possible there should be different GCS buckets for different workflows or workflow executions, in which case the instructions below would need to be rerun to point to that bucket. This generally follows the GCP docs to [create a persistent volume](https://cloud.google.com/kubernetes-engine/docs/how-to/persistent-volumes/cloud-storage-fuse-csi-driver#create-persistentvolume) and [create a persistent volume claim](https://cloud.google.com/kubernetes-engine/docs/how-to/persistent-volumes/cloud-storage-fuse-csi-driver#create-persistentvolumeclaim).

1. Modify the manifests (if needed, e.g. to point to your GCS bucket. See the docs for more info about requirements):
    * Example persistent volume: [manifests/persistent_volume.yaml](manifests/persistent_volume.yaml)
    * Example persistent volume claim: [manifests/persistent_volume_claim.yaml](manifests/persistent_volume_claim.yaml)
2. Create the persistent volume:
    ```
    $ kubectl apply -f manifests/persistent_volume.yaml
    ```
3. Create the persistent volume claim:
    ```
    $ kubectl apply -f manifests/persistent_volume_claim.yaml
    ```

## Running the script

The setup creates a leader pod in which the main [parsl_workflow.py](parsl_workflow.py) script is executed. During the script execution, parsl will bring up additional worker pods as needed. These worker pods need to be able to communicate back to the main script, so that’s the reason we run it in the leader pod (since networking restrictions allow easier communication between pods within the cluster than outside of it).

> **TODO:** An alternative setup that the QGreenland project uses is to run a Kubernetes Job with a ConfigMap that runs the script, which we could try also. See comments in the [QGreenland parsl repo](https://github.com/QGreenland-Net/parsl-exploration/blob/main/README.md#submitting-jobs-to-a-remote-cluster).

At the moment, both the leader and worker pods use the same Docker image, but this is not necessary - we could maintain separate images for the leader and worker instead (for example if we wanted to add the command to execute the script to the leader’s Dockerfile). Note that both are required to use the same parsl version. Note that the worker pods are automatically created with an IfNotPresent pull policy so a new image tag needs to be used every time the worker pod image changes.

### Steps

1. Make changes to the worker pod configurations. The worker pods are configured in the script itself, so this changes the code and requires the Docker image to be rebuilt in step 2
    * Parameters to configure the worker pods are in [parsl_config.py](parsl_config.py)
    * Things to note:
        - pod name prefix: `viz-workflow-worker`
        - `image` **should** be set to the tag that will be used in step 2
        - The worker pods need to be set up to consume the GCS persistent volume. `persistent_volumes` has been set to the point to the persistent volume claim from [One-time setup](#one-time-setup) above, and the mount path **should** match the directories used for input/output data in [workflow_config.py](workflow_config.py). In addition, a service account and a particular annotation must be provided in order for the worker pods to consume the GCS persistent volume, per the GCP docs on [mounting persistent volumes](https://cloud.google.com/kubernetes-engine/docs/how-to/persistent-volumes/cloud-storage-fuse-csi-driver#consume-persistentvolumeclaim); this has been done by setting `service_account_name` to the service account from [One-time setup](#one-time-setup) above and including the annotation `{"gke-gcsfuse/volumes": "true"}` in `annotations`
        - `namespace` has been set to the namespace from [One-time setup](#one-time-setup) above
2. Make any other changes to the code and rebuild the Docker image replacing `<tag>` below
    ```
    $ docker build -t ghcr.io/permafrostdiscoverygateway/viz-workflow:<tag> .
    $ docker push ghcr.io/permafrostdiscoverygateway/viz-workflow:<tag>
    ```
3. Create or modify the leader deployment manifest
    * Example deployment: [manifests/leader_deployment.yaml](manifests/leader_deployment.yaml)
    * Things to note:
        - deployment name (and container name): `viz-workflow-leader`
        - `image` **should** be set to the tag from step 2
        - The leader deployment needs to be set up to consume the GCS persistent volume. `volumeMounts` and `volumes` have been set to point to the persistent volume claim from [One-time setup](#one-time-setup) above, and similar to the worker pod config `mountPath` **should** match the directories used for input/output data in [workflow_config.py](workflow_config.py). In addition, `service_account_name` and `annotations` must be provided in the same way as for the worker pods above
        - `namespace` has been set to the namespace from [One-time setup](#one-time-setup) above
4. Create or update the leader deployment
    ```
    $ kubectl apply -f manifests/leader_deployment.yaml
    ```
5. Open a terminal within the leader pod and execute the script in that terminal. The pod name changes every time the deployment is updated so replace `<pod_name>` below with the current name
    ```
    $ kubectl exec -it <pod_name> -c viz-workflow-leader -n viz-workflow -- bash
    $ python parsl_workflow.py
    ```

## Cleanup

1. From the Cloud Console GKE Workloads page, delete any worker pods. Usually the parsl script itself should clean up these pods at the end of a run, but you may need to do it manually if the previous run exited abnormally:
    * pod_name prefix: `viz-workflow-worker`
2. *Optional:* From the Cloud Console GKE Workloads page, delete the leader pod:
    * deployment name: `viz-workflow-leader`
3. *Optional:* From Cloud Console GCE VM Instances page, stop the GCE VM instance:
    * instance name: `pdg-gke-entrypoint`
    * zone: `us-west1-b`
