#!/usr/bin/env bash
set -euo pipefail

# Send our parameters files to the cluster. This will update the ConfigMap if
# there are any changes. Note that "age" represents the time since the
# ConfigMap was created, not since it was last updated.
kubectl create configmap viz-workflow-cm --from-file parameters/ \
    -o yaml -n viz-workflow --dry-run=client \
    | kubectl apply -f -

# Submit a "Job" to the cluster which runs our script
kubectl apply -f manifests/leader_job.yaml
