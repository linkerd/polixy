# Linkerd policy exploration

## Problem

Linkerd's proxies establish an _identity_ based on Kubernetes's
`ServiceAccounts` system. This identity allows clients to establish
mutually-authenticated TLS connections between pods. Currently, this identity
is opportunistic and operators have little means to require its use.

This document proposes a mechanism for operators to require mTLS
communication and to enforce authorization for pod-to-pod communication.

## Goals

1. Provides mechanisms for application operators to restrict access to
   their servers.
2. Does not complicate getting started with Linkerd.
3. Leverages existing Kubernetes primitives/patterns wherever possible.
4. Identifies primitives/patterns that can be re-used for other types of
   configuration.

## Design

### Server-centric

Access policies obviously need to be enforced on the server-side in order to
provide any reasonable guarantees against malfeasant clients. As such, we
need a means for an inbound proxy to discover policies for its local servers
(ports that the pod-local containers listen on).

While it may be attractive to use Kubernetes `Service`s for this, they are
really not the right tool for the job. Kubernetes services are more of a
client-centric concept than a server-centric concept: they define a logical
target for traffic independent of its actual destination. A server, however,
has no way to correlate an inbound connection with a service. And it may
receive traffic that isn't associated with a `Service` at all (kubelet
probes, for instance).

This points to the introduction of a new custom resource type that
describes a *server*--matching a port on a set of pods.

#### Dynamic Policy Discovery

Outbound proxies perform service discovery based on the target IP:Port of a
connection. Inbound proxies will similarly need to watch for policy changes.

Outbound proxies are lazy and dynamic, as we cannot require an application to
document all endpoints to which an application connects; but, on the inbound
side, it's much more reasonable to expect operators to document the ports on
which an application accepts connections. In fact, this almost always done as
part of a `Pod` spec.

This means that we can likely configure a proxy (at inject-time) with a list
of ports for which the inbound proxy may accept connections. Then, the proxy
can watch policy updates for each of these ports. This could be completed
before a proxy starts accepting connections and marks itself as _ready_.

The inbound policy API (as well as a validating admission controller) should
probably be served from the destination controller's pod--proxies already
have a client to this service, and a policy API does not need any special
privileges that we would not want the destination controller to have
otherwise.

##### Controller Bootstrapping

The above scheme poses a "*Wyld Stallyns* problem" for the identity
controller: the identity controller needs to discover inbound policy in order
to start issuing certificates, but the destination controller cannot accept
connections until it obtains a certificate from the identity controller.

We want the identity controller to remain in a distinct deployment, separate
from the other controller containers, as it requires access to signing
secrets that these other processes should not be able to access.

We'll need to figure out a way for the identity controller to startup without
requiring access to the destination controller. One approach could be to
serve a specialized version of the API endpoints--only for the identity
controller's proxy--from the identity controller. This only feasible because
the identity controller's proxy has very limited discovery needs:

* It only initiates outbound connections to the Kubernetes API (on 443).
* It needs to discover policy for its local ports (identity gRPC + admin, proxy
  ports)
* It attempts to discover a service profile for inbound gRPC requests

### Authorizing clients

We've identified the need for _server_ resources; but we haven't yet
described how access policy is defined.

When policy is configured for a server, connections are denied by default.
This supports a secure default and allows authorizations to be purely
addititive. This eliminates any need for ordering/precedence in authorization
policies. Authoriztions simply grant access for a class of clients to connect
to a server (or servers).

There are two fundamental classes of clients:

1. Clients authenticated with Linkerd's mutual identity (via mTLS)
2. Unauthenticated clients

Authenticated clients may be matched by `ServiceAccount` (because Linkerd's
identity system builds on `ServiceAccount`s) or, in order to support
multicluster use cases where no local `ServiceAccount` exists for a client,
raw Linkerd identity strings may be used to identify clients.

Unauthenticated clients may be permitted with source-IP restrictions. The
most common case for this is to support connections from the local kubelet,
which runs on the local node's host network.

#### Default behavior

When no policy is configured for a server...

#### Allowing unauthenticated access for kubelet

Kubelet is a node-level process responsible for probing pods to report their
status to controllers. It's essential that kubelet be able to access health
checking ports; but

#### Control plane policies

#### Proxy admin, tap, & inbound policies

### Visibility

## Proposal

### Resources

We propose introducing two new `CustomResourceDefinition`s to Linkerd:

#### [`Server`](crds/server.yml)

Each `Server` instance:

* Selects over pods by label
* Matches a single port by name or value
* Optionally indicates how the proxy should detect the protocol of these
  streams

It's possible for multiple `Server` instances to conflict by matching the
same workloads + ports, much in the way that it's possible for multiple
`Deployment` instances to match the same pods. This behavior is undefined. We
cannot necessarily detect this situation at `Server`-creation time (due to
the nature of label selector expressions), so this situation should be
flagged by `linkerd check`. It may also make sense to try to handle this
in an admission controller to prevent resources from being created in this
state, but it may be difficult to provide exhaustive defenses agains this
situation.

#### [`Authorization`](crds/authz.yml)

Authorizes clients to access `Server`s

![Policy resources](./img/resources.png "Policy resources")

## Future work

- Cluster-wide policies
- Egress policies
- Timeouts
- HTTP Route configuration
- View isolation in the destination service

## Prior art

- SMI `TrafficPolicy` -- not port-aware; not workload-aware.
- `serviceprofiles.linkerd.io/ServiceProfile` -- not port-aware; tied to DNS (service) names
- `NetworkPolicy` -- not authentication-aware
- `Role`/`ClusterRole`, `RoleBinding`/`ClusterRoleBinding` -- requires authentication
