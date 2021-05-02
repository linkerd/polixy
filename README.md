# polixy

A prototype of policy for Linkerd.

See [DESIGN.md](./DESIGN.md) for details.

## Requires

* Kubernetes 1.16+;
* [Linkerd 2.10+](linkerd.io);

## Running

### Install CRDs

```console
:; kubectl apply -f ./crds
customresourcedefinition.apiextensions.k8s.io/authorizations.polixy.olix0r.net configured
customresourcedefinition.apiextensions.k8s.io/servers.polixy.olix0r.net configured
```

### Install example application

```sh
:; kubectl apply -f ./emojivoto/ns.yml && kubectl apply -f ./emojivoto
namespace/emojivoto created
server.polixy.olix0r.net/prom created
authorization.polixy.olix0r.net/prom-prometheus created
serviceaccount/emoji created
service/emoji created
deployment.apps/emoji created
server.polixy.olix0r.net/emoji-grpc created
authorization.polixy.olix0r.net/emoji-grpc created
namespace/emojivoto unchanged
server.polixy.olix0r.net/prom unchanged
authorization.polixy.olix0r.net/prom-prometheus unchanged
serviceaccount/web created
deployment.apps/vote-bot created
serviceaccount/voting created
service/voting created
deployment.apps/voting created
server.polixy.olix0r.net/voting-grpc created
authorization.polixy.olix0r.net/voting-grpc created
serviceaccount/web configured
service/web created
deployment.apps/web created
server.polixy.olix0r.net/web-http created
authorization.polixy.olix0r.net/web-public created
```

### Run the controller

```sh
:; cargo run -- controller
```

### Run a client

```sh
:; pod=$(kubectl get -n emojivoto po -l app.kubernetes.io/name=web -o 'jsonpath={.items[*].metadata.name}')
:; cargo run -- client watch -n emojivoto $pod 8081
```
