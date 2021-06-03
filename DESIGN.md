# Linkerd policy exploration

Linkerd proxies inbound (server-side) communication to pods and is well-positioned to enforce
policies that place restrictions on the clients that may access it.

All proxy-to-proxy communication is already protected by mutually-authenticated TLS, with identities
based on each pod's `ServiceAccount`. Currently, this identity is opportunistic and operators have
little means to require its use or to limit access based on this identity.

This document proposes a mechanism for operators to configure inbound proxies enforce authorization
for pod-to-pod communication.

## Design

### Goals

* Define a mechanism for servers operators to authorize clients;
* Integrate with Linkerd's identity system;
* Opt-in -- not required, especially when getting started;
* Can be adopted incrementally;
* Leverage existing Kubernetes primitives/patterns;
* Identify reusable components for future server configuration;
* Keep proxy-facing Kubernetes-agnostic;
* Support interoperability with SMI `TrafficPolicy`;
* No measurable impact to HTTP request latencies; and
* Negligible impact to proxy memory usage.

### Non-goals

#### Cluster-wide policy

Cluster-level policy control should be implemented by operators that generate or restrict
application-level policy resources. Schemes for implementing these (e.g., OPA/Gatekeeper) are out of
scope of this document.

#### Policy based on HTTP metadata

While we shouldn't preclude future support for other types of protocol-aware policy, this initial
design is only intends to address connection-level policy.

### Server-centric

Access policies need to be enforced on the server-side in order to provide any reasonable guarantees
against malfeasant clients. As such, we need a means for an inbound proxy to discover policies for
its local servers (ports that the pod-local containers listen on).

While it may be attractive to use Kubernetes `Service`s for this, they are really not the right tool
for the job: Kubernetes services represent a traffic target _for clients_. A server instance,
however, has no way to correlate an inbound connection with the service the client targeted; and it
may receive traffic that isn't associated with a `Service` at all.

This points to the introduction of a new custom resource type that describes a *server*--matching a
port on a set of pods.

#### Dynamic Policy Discovery

Outbound proxies perform service discovery based on the target IP:Port of a connection. Inbound
proxies will similarly need to adapt to policies at runtime (i.e., without requiring that the proxy
be restarted).

Outbound proxies are lazy and dynamic, as we cannot require applications to document all endpoints
to which they may connect; but, on the inbound side, it's reasonable to expect operators to document
the ports on which an application accepts connections. In fact, this almost always done in each
pod's spec.

This will allow proxies to discovery policies at startup-time and block the pod's readiness based on
this availability.

#### Protocol hinting

Linkerd 2.10 introduced the `config.linkerd.io/opaque-ports` annotation that configures server-side
ports to skip protocol detection. With the introduction of a server descriptor, we have an
opportunity to extend this configuration even further by allowing operators to document the protocol
being proxied to avoid detection overhead (and their related timeouts).

Clients can similarly discover these protocols via the Destination API.

### Authorizing clients

Clients fall into two broad categories:

1. Meshed clients communicating with mutual-authenticated TLS;
2. Un-meshed clients communicating without (mesh-aware) authentication.

#### Authenticated clients

Meshed clients automatically authenticate to servers via mTLS so that the client's identity is
available to the server. An operator may restrict access to all authenticated clients or a subset of
authenticated clients.

Authenticated clients may be matched by `ServiceAccount` or, more generally, Linkerd identity names.
DNS-like identity names are encoded into each proxy's certificate, and each certificate is created
using the pod's `ServiceAccount` in the form
`<serviceaccount>.<namespace>.serviceaccount.identity.linkerd.<identity-domain>`.

It's most natural to authorize authenticated clients by referring to service accounts directly;
however, we probably also want to support matching identity names as well. For instance, when
authorizing clients to connect to multi-cluster gateways, we cannot reference service accounts in
other clusters. Instead we might want to express matches like `*.<identity-domain>` to match all
clients in an identity domain or `*.<namespace>.serviceaccount.identity.linkerd.<identity-domain>` to match
all clients in a specific namespace.

#### Lifecycle probes

In Kubernetes, _kubelet_ is a process that runs on each node and is responsible for orchestrating a
pod's lifecycle: it executes and terminates a pod's containers and, more importantly for our needs,
it may issue networked probes to know when a container is _ready_ or _live.

It is not feasible to configure kubelet to run within the mesh, so we can only identify this traffic
by its source IP.  Kubelet initiates network probes from the first address on the node's pod
network--e.g., if the node's `podCIDR` is `10.0.1.0/24`, then the kubelet will initiate connections
from `10.0.1.1`. (See [this blog post on pod networking][pod-ips] for more information.)

If a policy were to block this communication, pods would not start properly. So we need to be
careful to allow this traffic by default to minimize pain. Furthermore, there's really no benefit to
disallowing communication from the kubelet--kubelet is necessarily a privileged application that
must be trusted by a pod.

#### Default behavior

We must not require that all servers are described, as this would dramatically complicate getting
started with Linkerd. To support incremental adoption of Linkerd in general, and specifically
policies, we need to allow traffic by default.

As soon as a Server is described, however, we can require that clients must be explicitly authorized
to communicate with the server.

But a default-allow policy isn't desirable from a security point-of-view. If an operator has taken
the time to document all servers in a cluster, they may not want a subsequent misconfiguration to
expose servers without authentication. So, we probably want to support a few different default
modes:

1. Allow unauthenticated from everywhere
2. Allow unauthenticated from within the cluster
3. Allow mesh-authenticated from everywhere
4. Allow mesh-authenticated from within the cluster
5. Deny

This default setting should be configurable at the control-plane-level, or per-workload via the
`polixy.linkerd.io/default-allow` annotation. The proxy injector should copy these annotations from
namespaces onto each workload so the controller only needs to track workload annotations for
discovery.

## Proposal

### Resources

We propose introducing two new `CustomResourceDefinition`s to Linkerd:

![Policy resources](./img/resources.png "Policy resources")

#### [`Server`](k8s/crds/server.yml)

Each `Server` instance:

* Selects over pods by label
* Matches ports by name or value
* Optionally indicates how the proxy should detect the protocol of these streams

##### `proxyProtocol: unknown`

If no proxy protocol is set (or `unknown` is set explicitly), the proxy's (HTTP) protocol detection
is performed. This is the default behavior in current proxy versions.

##### `proxyProtocol: opaque`

Equivalent to setting the port in `config.linkerd.io/opaque-ports` -- indicates that the server
should not do any protocol detection (and neither should meshed clients).

##### `proxyProtocol: TLS`

Indicates that the server terminates TLS. The proxy may require that all connections include a TLS
ClientHello and it should skip HTTP-level detection.

##### `proxyProtocol: HTTP/1 | HTTP/2 | gRPC`

Indicates that the server supports the referenced HTTP variant. gRPC is provided as a special case
for HTTP/2 to support future specialization.

##### Handling conflicts

It's possible for multiple `Server` instances to conflict by matching the same workloads + port,
much in the way that it's possible for multiple `Deployment` instances to match the same pods. This
behavior is undefined.  Operators must not create conflicting servers.

It should be possible to detect this situation at `Server`-creation time--at least, we should be
able to detect overlapping label selectors for the same port. It may **not** be feasible to reliably
detect servers that match the same _port_, however, as named ports may only conflict with numbered
pots at pod-creation time. So, the validating webhook could potentially prevent the creation of
these pods, or we'll need to implement CLI checks that detect this situation.

#### [`ServerAuthorization`](k8s/crds/authz.yml)

Authorizes clients to access `Server`s.

* References servers in the same namespace by name or label selector.
* Scoped to source IP networks. If no networks are specified, the authorization applies to clients
  in all networks.
* Indicates whether connections may be unauthenticated (i.e. without mesh TLS); or
* Expresses mesh TLS requirements:
  * By referencing service accounts (in arbitrary namespaces); or
  * By matching identity strings (including globbed suffix matches); or
  * Not requiring client identities at all -- only relevant for the `identity` controller that must
    serve requests to clients that have not yet obtained an identity.

### Overview

* A new _server policy_ controller is added to the control plane, responsible for serving a gRPC API
  to proxies for discovery and for validating resources as they are created (via
  `ValidatingAdmissionWebhook`).
* The proxy injector is modified to configure proxies with:
  * The location & identity of the API server;
  * A "workload coordinate", potentially reusing the destination controller's "context token", which
    encodes at least the namespace and pod name.
  * A comma-separated list of numeric container ports for the pod.
  * The proxy does not permit connections for ports that are not documented in the pod spec.
  * The proxy no longer forwards inbound connections on localhost. Instead, the discovered
    configuration indicates the IPs on which connections are permitted, and the proxy only forwards
    connections targeting these IPs. This may interfere with complicated networking schemes (e.g.
    Docker-in-Docker); but we're probably better off figuring out how to support these networking
    overlays in proxy-init, etc.
  * Protocol detection is informed by discovery:
    * HTTP:
      * When a connection is authorized, requests are [annotated with headers](#headers).
      * When a connection is not authorized, HTTP responses are emitted with the status `403 Forbidden`.
    * gRPC:
      * When a connection is authorized, requests are [annotated with headers](#headers).
      * When a connection is not authorized, gRPC responses are emitted with a header
        `grpc-status: PERMISSION_DENIED`
  * Unauthenticated connections are _always_ permitted from the kubelet.

#### HTTP/gRPC headers <a name="headers"></a>

Proxies should surface informational headers to the application describing authorized clients for
Servers with a `proxyProtocol` value of `HTTP` or `gRPC`.

Use of these headers may be disabled by setting a server annotation:

```yaml
apiVersion: polixy.linkerd.io/v1alpha1
kind: Server
metadata:
  annotations:
    polixy.linkerd.io/http-informational-headers: disabled
```

##### `l5d-connection-secure: true | false`

The `l5d-connection-secure` indicates whether the client connected to the server via meshed TLS.
When the value is `true`, the `l5d-client-id` header may also be set to indicate the client's
identity.

This header is always set by the proxy (when informational headers are not disabled).

##### `l5d-client-id: <client-id>`

The `l5d-client-id` header is only set when the client has been authenticated via meshed TLS. Its
value is the client's identity, e.g. `default.default.serviceaccount.identity.linkerd.cluster.local`.

##### `forwarded: for=<client-ip>;by=<server-addr>`

[RFC 7239](https://tools.ietf.org/html/rfc7239) standardizes use of the `forwarded` header to
replace `x-forwarded-*` headers. In order to inform the client of the client's IP address, the proxy
appends a `

#### Identity Controller Bootstrapping

The above scheme poses a "*Wyld Stallyns* problem" for the identity controller: the identity
controller needs to discover inbound policy in order to start issuing certificates, but the
destination controller cannot accept connections until it obtains a certificate from the identity
controller.

We want the identity controller to remain in a distinct deployment, separate from the other
controller containers, as it requires access to signing secrets that these other processes should
not be able to access.

We'll need to figure out a way for the identity controller to startup without requiring access to
the destination controller. One approach could be to serve a specialized version of the API
endpoints--only for the identity controller's proxy--from the identity controller. This only
feasible because the identity controller's proxy has very limited discovery needs:

* It only initiates outbound connections to the Kubernetes API (on 443).
* It needs to discover policy for its local ports (identity gRPC + admin, proxy ports)
* It attempts to discover a service profile for inbound gRPC requests

#### Control plane policies

The core control plane should ship with a set of default policies:

* The destination controller requires mutually authenticated requests.
  [[k8s/linkerd/destination.yml](./k8s/linkerd/destination.yml)]
* The identity controller requires secured connections that may not be
  authenticated (because clients have not yet received identity).
  [[k8s/linkerd/identity.yml](./k8s/linkerd/identity.yml)]
* Webhook connections must use TLS.
  [[k8s/linkerd/proxy-injector.yml](./k8s/linkerd/proxy-injector.yml)]
* Admin server connections must be authenticated or originate from the
  node-local network. [[k8s/linkerd/proxy-injector.yml](./k8s/linkerd/proxy-injector.yml)]

<!--
#### Proxy admin, tap, & inbound policies
  -->

### Why not `access.smi-spec.org`?

There are a few things that don't... mesh ;)

#### Ports

SMI isn't port-aware. Our `Server` abstraction gives us a useful, extensible building block that
allows us to attach configuration to pod-ports. In the same way that we can attach authorizations to
a `Server`, we'll be able to extend the server API to support, for instance, HTTP routes, gRPC
services, etc.

#### Destinations

SMI binds policy to destinations as follows:

```yaml
kind: TrafficTarget
metadata:
  name: target
  namespace: default
spec:
  destination:
    kind: ServiceAccount
    name: service-a
    namespace: default
  ...
```

This is a bit awkward for a few reasons:

* These targets need not exist in the same namespace as the policies? So it appears as if policies
  can be created in unrelated namespaces (by unrelated owners), and it's not clear how these
  policies should be applied.
* While it makes sense for us to bind clients to `ServiceAccounts`--this is how we authenticate pods
  to the identity service--it's unnatural and unnecessary to do this for servers. All pods that
  share a service account need not have the same access patterns. For instance, it's common for all
  pods in a namespace to share a common (`default`) `ServiceAccount`, though the pods serve varying
  APIs to multitude of clients.

We _really_ want to bind policies to pod-and-port pairs (as described by our `Server` resource). And
we _really_ want all authorizations to _only_ be defined in the same namespace as the server. It
makes no sense to support (inbound) policies defined in other namespaces--only a service's owners
can define its access policies.

## Open questions

* What should we call the API group? `polixy` is a placeholder (policy + olix0r). We should change
  this to something a bit more concrete. This name should probably match the controller's name.
* How do we provide interop with SMI? I.e. something that reads TrafficTarget resources and
  generates Linkerd resources (and vice-versa?). It will probably be clunky but it seems doable.
* How will we support HTTP routes & gRPC services? How does authorization work for these?
* What Linkerd CLI tools do we need to interact with policies?
* Do we need `check`s for policies?
* How are policies reflected in metrics/tap?
* How do we restrict requests to the controller? I.e. API clients should not be able to request
  policies for servers they do not serve; but we still may need to support non-proxy clients for
  tooling.
* How do policies interact with the multi-cluster gateway?
* How do policies interact with tap servers?
* How do policies interact with admin servers?
* Do we want to stick with a [controller written in Rust](./src)? Or would it be better to
  re-implement this with `client-go`?

## Implementation

### Control-plane

#### Injector

* Set `LINKERD2_PROXY_INBOUND_CONTEXT` env with a namespace and pod name (similarly to
  `LINKERD2_PROXY_DESTINATION_CONTEXT`).
* Set `LINKERD2_PROXY_INBOUND_PORTS` env with a comma-separated list of all ports documented on the
  pod, including proxy ports.
* Set `LINKERD2_PROXY_INBOUND_IPS` env to a comma-separated lits of all podIPs.
* Set the `inbound.linkerd.io/default-allow` annotation when it is not set. Either from the
  namespace or the cluster-wide default.
* Set `LINKERD2_PROXY_INBOUND_DEFAULT_ALLOW` env with the same value.

### Proxy

1. Modify proxy initialization to load per-connection policies. This should initially encapsulate
   the opaque-ports configuration.
2. Use the `INBOUND_IPS` setting to restrict which target ips are permitted on inbound connections.
   * Stop rewriting the inbound target ip address to 127.0.0.1.
3. Modify controller clients to be cached like outbound clients. A proxy may or may not be configured
   configured independently from the inbound controller--especially the identity controller, which
   needs to be able to discover inbound policy locally before a destination pod is available. The
   controller should be a `NewService` that accepts a target that specifies the target addr/tls.

### Controller

* Extract `linkerd-drain` into a distinct, versioned [crate](https://crates.io/crates/drain)  so it
  can be used by the controller without git dependencies.
* Add indexer metrics.
* Support a mode where watches are namespace-scoped instead of cluster-scoped. So that the identity
  controller's instance need not cache the whole cluster's information.

## Future work

* HTTP route authorization
* Egress policies
* View isolation in the destination service

<!-- references -->
[pod-ips]: https://web.archive.org/web/20201211005235/https://ronaknathani.com/blog/2020/08/how-a-kubernetes-pod-gets-an-ip-address/
