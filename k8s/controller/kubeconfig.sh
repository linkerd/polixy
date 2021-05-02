#!/bin/sh

# Modified from https://gist.githubusercontent.com/innovia/fbba8259042f71db98ea8d4ad19bd708/raw/fd1c267488c2cf9aaecb541548e814cb41b23e03/kubernetes_add_service_account_kubeconfig.sh

set -eu

ns="polixy"
sa="controller"

# Extract metadata for the current context.
context=$(kubectl config current-context)
cluster=$(kubectl config view -o jsonpath="{.contexts[?(@.name ==\"${context}\")].context.cluster}")
server=$(kubectl config view -o jsonpath="{.clusters[?(@.name == \"${cluster}\")].cluster.server}")

user="${context}-${ns}-${sa}"

dir="${1:-./target/${user}}"

# If the kubeconfig already exists, don't regenerate it.
if [ -f "$dir/config" ]; then
    echo "$dir/config"
    exit
fi
mkdir -p "$dir"

# A helper to act on the a new kubeconfig
kconfig() {
    kubectl config --kubeconfig="${dir}/config" "$@"
}

## Generate a Kubeconfig:
(
    secret=$(kubectl -n "${ns}" get sa "${sa}" -o json | jq -r '.secrets[].name')

    # Extract the cluster's CA certificate from the ServiceAccount secret and ensure
    # it's embedded in the kubeconfig's context.
    kubectl -n "${ns}" get secret "${secret}" -o json \
        | jq  -r '.data["ca.crt"] | @base64d' \
        > "${dir}/ca.crt"
    kconfig set-cluster "${cluster}" \
        --server="${server}" \
        --certificate-authority="${dir}/ca.crt" \
        --embed-certs=true \

    # Embed the service account token credentials
    token=$(kubectl -n "${ns}" get secret "${secret}" -o json  |jq -r '.data["token"] | @base64d')
    kconfig set-credentials "${user}" --token="${token}"

    # Create & activate a new context.
    kconfig set-context "${user}" \
        --cluster="${cluster}" \
        --user="${user}" \
        --namespace="${ns}"
    kconfig use-context "${user}"
) >/dev/null

echo "${dir}/config"
