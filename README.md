# Linkerd policy exploration

## Problem

Linkerd's proxies establish an _identity_ based on Kubernetes's
`ServiceAccounts` system. This identity allows clients to establish
mutually-authenticated TLS connections between pods. Currently, this identity
is opportunistic and operators have little means to require its use.

This document proposes a mechanism for operators to require mTLS
communication and to enforce authorization for pod-to-pod communication.

## Goals

1. Provides a means for cluster operators to enforce cluster-wide access policies.
2. Provides a means for application operators to enforce application-level
   access policies.
3. Does not complicate getting started with Linkerd.
4. Leverages existing Kubernetes primitives/patterns wherever possible.
5. Identifies primitives/patterns that can be re-used for other types of
   configuration.
6. Supports compability with SMI's `TrafficPolicy` API.

## Design

### Proxy configuration/discovery

Access policies obviously need to be enforced on the server-side in order to
provide any reasonable guarantees against malfeasant clients. And, while the
proxy performs per-destination discovery its outbound side, no such
serverside logic exists presently. (The proxy does perform service profile
discovery based on inbound HTTP request metadata, but these lookups do not
necessarily block requests and only apply to HTTP requests).

#### Bootstrapping and identity

The identity controller, in particular, must not depend on any external services.

### Policies

#### Requiring authenticated communication

##### Inbound

#### Limiting Authorized Clients

#### Allowing unauthenticated access for kubelet

Kubelet is a node-level process--that cannot be meshed with Linkerd--
responsible for probing pods to report their status to controllers. As such, we need to

## Future work

- Egress policies
- Timeouts
- HTTP Route configuration
- View isolation in the destination service

## Prior art

- SMI `TrafficPolicy`
- `serviceprofiles.linkerd.io/ServiceProfile`
- `Role`/`ClusterRole`, `RoleBinding`/`ClusterRoleBinding`
- `NetworkPolicy`
