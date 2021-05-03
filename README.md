# polixy

A prototype of policy for Linkerd.

See [DESIGN.md](./DESIGN.md) for details.

## Requires

* A Kubernetes 1.16+ cluster, available via kubectl;
* [Linkerd 2.10+](linkerd.io)--so that workloads are labeled appropriately;

## Running

### Install `polixy.olix0r.net` CRDs

```sh
:; kubectl apply -f ./k8s/crds
```

### Run the controller locally

We create a new `polixy` namespace with a `controller` ServiceAccount, with
limited cluster access, and extract a kubeconfig to the local filesystem to use with the controller:

```sh
:; kubectl apply -f ./k8s/controller/sa.yml
:; KUBECONFIG=$(./k8s/controller/kubeconfig.sh) cargo run -- controller
```

### Install example application (with policies)

```sh
:; kubectl apply -f ./k8s/emojivoto/ns.yml && kubectl apply -f ./k8s/emojivoto
```

### Run a client

```sh
:; pod=$(kubectl get -n emojivoto po -l app.kubernetes.io/name=web -o 'jsonpath={.items[*].metadata.name}')
:; cargo run -- client get -n emojivoto $pod 8080
```

```sh
:; pod=$(kubectl get -n emojivoto po -l app.kubernetes.io/name=voting -o 'jsonpath={.items[*].metadata.name}')
:; cargo run -- client get -n emojivoto $pod 8080
```

```sh
:; pod=$(kubectl get -n emojivoto po -l app.kubernetes.io/name=voting -o 'jsonpath={.items[*].metadata.name}')
:; cargo run -- client get -n emojivoto $pod 8801
`
