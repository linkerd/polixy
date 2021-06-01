use super::*;
use crate::{k8s::polixy::server::Port, *};
use ipnet::{IpNet, Ipv4Net, Ipv6Net};
use std::{str::FromStr, sync::Arc};
use tokio::time;

/// Creates a pod, then a server, then an authorization--then deletes these resources in the reverse
/// order--checking the server watch is updated at each step.
#[tokio::test]
async fn incrementally_configure_server() {
    let cluster_net = IpNet::from_str("192.0.2.0/24").unwrap();
    let pod_net = IpNet::from_str("192.0.2.2/28").unwrap();
    let (kubelet_ip, pod_ip) = {
        let mut ips = pod_net.hosts();
        (ips.next().unwrap(), ips.next().unwrap())
    };
    let detect_timeout = time::Duration::from_secs(1);
    let mut idx = Index::new(
        vec![cluster_net],
        DefaultAllow::ClusterUnauthenticated,
        detect_timeout,
    );
    let (mut lookup_tx, lookup_rx) = crate::lookup::pair();

    idx.apply_node(mk_node("node-0", pod_net)).unwrap();

    let pod = mk_pod(
        "ns-0",
        "pod-0",
        "node-0",
        pod_ip,
        Some(("container-0", vec![2222, 9999])),
    );
    idx.apply_pod(pod.clone(), &mut lookup_tx).unwrap();

    let default_config = InboundServerConfig {
        authorizations: mk_default_allow(DefaultAllow::ClusterUnauthenticated, cluster_net),
        protocol: crate::ProxyProtocol::Detect {
            timeout: detect_timeout,
        },
    };

    // A port that's not exposed by the pod is not found.
    assert!(lookup_rx.lookup("ns-0", "pod-0", 7000).is_none());

    // The default policy applies for all exposed ports.
    let mut port2222 = lookup_rx.lookup("ns-0", "pod-0", 2222).unwrap();
    assert_eq!(port2222.pod_ips, PodIps(Arc::new([pod_ip])));
    assert_eq!(port2222.kubelet_ips, KubeletIps(Arc::new([kubelet_ip])));
    assert_eq!(*port2222.rx.borrow().borrow(), default_config);

    // In fact, both port resolutions should point to the same data structures (rather than being
    // duplicated for each pod).
    let port9999 = lookup_rx.lookup("ns-0", "pod-0", 9999).unwrap();
    assert!(Arc::ptr_eq(&port9999.pod_ips.0, &port2222.pod_ips.0));
    assert!(Arc::ptr_eq(
        &port9999.kubelet_ips.0,
        &port2222.kubelet_ips.0
    ));
    assert_eq!(*port9999.rx.borrow().borrow(), default_config);

    // Update the server on port 2222 to have a configured protocol.
    let srv = {
        let mut srv = mk_server("ns-0", "srv-0", Port::Number(2222), None, None);
        srv.spec.proxy_protocol = Some(k8s::polixy::server::ProxyProtocol::Http1);
        srv
    };
    idx.apply_server(srv.clone());

    // Check that the watch has been updated to reflect the above change and that this change _only_
    // applies to the correct port.
    let basic_config = InboundServerConfig {
        authorizations: Default::default(),
        protocol: crate::ProxyProtocol::Http1,
    };
    assert_eq!(*port2222.rx.borrow().borrow(), basic_config);
    assert_eq!(*port9999.rx.borrow().borrow(), default_config);

    // Add an authorization policy that selects the server by name.
    let authz = {
        let mut az = mk_authz("ns-0", "authz-0", "srv-0");
        az.spec.client = k8s::polixy::authz::Client {
            mesh_tls: Some(k8s::polixy::authz::MeshTls {
                unauthenticated_tls: true,
                ..Default::default()
            }),
            ..Default::default()
        };
        az
    };
    idx.apply_authz(authz.clone()).unwrap();

    // Check that the watch now has authorized traffic as described above.
    assert_eq!(
        *port2222.rx.borrow().borrow(),
        InboundServerConfig {
            protocol: ProxyProtocol::Http1,
            authorizations: Some((
                "authz-0".into(),
                ClientAuthz {
                    authentication: ClientAuthn::TlsUnauthenticated,
                    networks: Arc::new([
                        ClientNetwork {
                            net: Ipv4Net::default().into(),
                            except: vec![],
                        },
                        ClientNetwork {
                            net: Ipv6Net::default().into(),
                            except: vec![],
                        },
                    ])
                }
            ))
            .into_iter()
            .collect(),
        }
    );

    // Delete the authorization and check that the watch has reverted to its prior state.
    idx.delete_authz(authz);
    assert!(matches!(
        time::timeout(time::Duration::from_secs(1), port2222.rx.changed()).await,
        Ok(Ok(()))
    ));
    assert_eq!(*port2222.rx.borrow().borrow(), basic_config);

    // Delete the server and check that the watch has reverted the default state.
    idx.delete_server(srv).unwrap();
    assert!(matches!(
        time::timeout(time::Duration::from_secs(1), port2222.rx.changed()).await,
        Ok(Ok(()))
    ));
    assert_eq!(*port2222.rx.borrow().borrow(), default_config);

    // Delete the pod and check that the watch recognizes that the watch has been closed.
    idx.delete_pod(pod, &mut lookup_tx).unwrap();
    assert!(matches!(
        time::timeout(time::Duration::from_secs(1), port2222.rx.changed()).await,
        Ok(Err(_))
    ));
}

// XXX this test currently fails due to a bug.
#[tokio::test]
async fn server_update_deselects_pod() {
    let cluster_net = IpNet::from_str("192.0.2.0/24").unwrap();
    let pod_net = IpNet::from_str("192.0.2.2/28").unwrap();
    let (kubelet_ip, pod_ip) = {
        let mut ips = pod_net.hosts();
        (ips.next().unwrap(), ips.next().unwrap())
    };
    let detect_timeout = time::Duration::from_secs(1);
    let mut idx = Index::new(
        vec![cluster_net],
        DefaultAllow::ClusterUnauthenticated,
        detect_timeout,
    );
    let (mut lookup_tx, lookup_rx) = crate::lookup::pair();

    idx.apply_node(mk_node("node-0", pod_net)).unwrap();
    let p = mk_pod(
        "ns-0",
        "pod-0",
        "node-0",
        pod_ip,
        Some(("container-0", vec![2222])),
    );
    idx.apply_pod(p, &mut lookup_tx).unwrap();

    let srv = {
        let mut srv = mk_server("ns-0", "srv-0", Port::Number(2222), None, None);
        srv.spec.proxy_protocol = Some(k8s::polixy::server::ProxyProtocol::Http2);
        srv
    };
    idx.apply_server(srv.clone());

    // The default policy applies for all exposed ports.
    let port2222 = lookup_rx.lookup("ns-0", "pod-0", 2222).unwrap();
    assert_eq!(port2222.pod_ips, PodIps(Arc::new([pod_ip])));
    assert_eq!(port2222.kubelet_ips, KubeletIps(Arc::new([kubelet_ip])));
    assert_eq!(
        *port2222.rx.borrow().borrow(),
        InboundServerConfig {
            authorizations: Default::default(),
            protocol: crate::ProxyProtocol::Http2,
        }
    );

    idx.apply_server({
        let mut srv = srv;
        srv.spec.pod_selector = Some(("label", "value")).into_iter().collect();
        srv
    });
    assert_eq!(
        *port2222.rx.borrow().borrow(),
        InboundServerConfig {
            authorizations: mk_default_allow(DefaultAllow::ClusterUnauthenticated, cluster_net),
            protocol: crate::ProxyProtocol::Detect {
                timeout: detect_timeout,
            },
        }
    );
}

/// Tests that pod servers are configured with defaults based on the global `DefaultAllow` policy.
///
/// Iterates through each default policy and validates that it produces expected configurations.
#[tokio::test]
async fn default_allow_global() {
    let cluster_net = IpNet::from_str("192.0.2.0/24").unwrap();
    let pod_net = IpNet::from_str("192.0.2.2/28").unwrap();
    let (kubelet_ip, pod_ip) = {
        let mut ips = pod_net.hosts();
        (ips.next().unwrap(), ips.next().unwrap())
    };
    let detect_timeout = time::Duration::from_secs(1);

    for default in &[
        DefaultAllow::Deny,
        DefaultAllow::AllAuthenticated,
        DefaultAllow::AllUnauthenticated,
        DefaultAllow::ClusterAuthenticated,
        DefaultAllow::ClusterUnauthenticated,
    ] {
        let mut idx = Index::new(vec![cluster_net], *default, detect_timeout);
        let (mut lookup_tx, lookup_rx) = crate::lookup::pair();

        idx.apply_node(mk_node("node-0", pod_net)).unwrap();

        let p = mk_pod(
            "ns-0",
            "pod-0",
            "node-0",
            pod_ip,
            Some(("container-0", vec![2222])),
        );
        idx.apply_pod(p, &mut lookup_tx).unwrap();

        let config = InboundServerConfig {
            authorizations: mk_default_allow(*default, cluster_net),
            protocol: crate::ProxyProtocol::Detect {
                timeout: detect_timeout,
            },
        };

        // Lookup port 2222 -> default config.
        let port2222 = lookup_rx.lookup("ns-0", "pod-0", 2222).unwrap();
        assert_eq!(port2222.pod_ips, PodIps(Arc::new([pod_ip])));
        assert_eq!(port2222.kubelet_ips, KubeletIps(Arc::new([kubelet_ip])));
        assert_eq!(*port2222.rx.borrow().borrow(), config);
    }
}

/// Tests that pod servers are configured with defaults based on the workload-defined `DefaultAllow`
/// policy.
///
/// Iterates through each default policy and validates that it produces expected configurations.
#[tokio::test]
async fn default_allow_annotated() {
    let cluster_net = IpNet::from_str("192.0.2.0/24").unwrap();
    let pod_net = IpNet::from_str("192.0.2.2/28").unwrap();
    let (kubelet_ip, pod_ip) = {
        let mut ips = pod_net.hosts();
        (ips.next().unwrap(), ips.next().unwrap())
    };
    let detect_timeout = time::Duration::from_secs(1);

    for default in &[
        DefaultAllow::Deny,
        DefaultAllow::AllAuthenticated,
        DefaultAllow::AllUnauthenticated,
        DefaultAllow::ClusterAuthenticated,
        DefaultAllow::ClusterUnauthenticated,
    ] {
        let mut idx = Index::new(
            vec![cluster_net],
            match *default {
                DefaultAllow::Deny => DefaultAllow::AllUnauthenticated,
                _ => DefaultAllow::Deny,
            },
            detect_timeout,
        );
        let (mut lookup_tx, lookup_rx) = crate::lookup::pair();

        idx.apply_node(mk_node("node-0", pod_net)).unwrap();

        let mut p = mk_pod(
            "ns-0",
            "pod-0",
            "node-0",
            pod_ip,
            Some(("container-0", vec![2222])),
        );
        p.annotations_mut()
            .insert(DefaultAllow::ANNOTATION.into(), default.to_string());
        idx.apply_pod(p, &mut lookup_tx).unwrap();

        let config = InboundServerConfig {
            authorizations: mk_default_allow(*default, cluster_net),
            protocol: crate::ProxyProtocol::Detect {
                timeout: detect_timeout,
            },
        };

        let port2222 = lookup_rx.lookup("ns-0", "pod-0", 2222).unwrap();
        assert_eq!(port2222.pod_ips, PodIps(Arc::new([pod_ip])));
        assert_eq!(port2222.kubelet_ips, KubeletIps(Arc::new([kubelet_ip])));
        assert_eq!(*port2222.rx.borrow().borrow(), config);
    }
}

/// Tests that an invalid workload annotation is ignored in favor of the global default.
#[tokio::test]
async fn default_allow_annotated_invalid() {
    let cluster_net = IpNet::from_str("192.0.2.0/24").unwrap();
    let pod_net = IpNet::from_str("192.0.2.2/28").unwrap();
    let (kubelet_ip, pod_ip) = {
        let mut ips = pod_net.hosts();
        (ips.next().unwrap(), ips.next().unwrap())
    };
    let detect_timeout = time::Duration::from_secs(1);

    let mut idx = Index::new(
        vec![cluster_net],
        DefaultAllow::AllUnauthenticated,
        detect_timeout,
    );
    let (mut lookup_tx, lookup_rx) = crate::lookup::pair();

    idx.apply_node(mk_node("node-0", pod_net)).unwrap();

    let mut p = mk_pod(
        "ns-0",
        "pod-0",
        "node-0",
        pod_ip,
        Some(("container-0", vec![2222])),
    );
    p.annotations_mut()
        .insert(DefaultAllow::ANNOTATION.into(), "bogus".into());
    idx.apply_pod(p, &mut lookup_tx).unwrap();

    // Lookup port 2222 -> default config.
    let port2222 = lookup_rx.lookup("ns-0", "pod-0", 2222).unwrap();
    assert_eq!(port2222.pod_ips, PodIps(Arc::new([pod_ip])));
    assert_eq!(port2222.kubelet_ips, KubeletIps(Arc::new([kubelet_ip])));
    assert_eq!(
        *port2222.rx.borrow().borrow(),
        InboundServerConfig {
            authorizations: mk_default_allow(DefaultAllow::AllUnauthenticated, cluster_net),
            protocol: crate::ProxyProtocol::Detect {
                timeout: detect_timeout,
            },
        }
    );
}

/// Tests observing a pod before its node has been observed.
///
/// TODO this shouldn't panic.
#[tokio::test]
#[should_panic]
async fn pod_before_node() {
    let cluster_net = IpNet::from_str("192.0.2.0/24").unwrap();
    let pod_net = IpNet::from_str("192.0.2.2/28").unwrap();
    let (_kubelet_ip, pod_ip) = {
        let mut ips = pod_net.hosts();
        (ips.next().unwrap(), ips.next().unwrap())
    };
    let detect_timeout = time::Duration::from_secs(1);

    let mut idx = Index::new(vec![cluster_net], DefaultAllow::Deny, detect_timeout);
    let (mut lookup_tx, _lookup_rx) = crate::lookup::pair();

    let p = mk_pod(
        "ns-0",
        "pod-0",
        "node-0",
        pod_ip,
        Some(("container-0", vec![2222, 9999])),
    );
    let _panics = idx.apply_pod(p, &mut lookup_tx);
}

fn mk_node(name: impl Into<String>, pod_net: IpNet) -> k8s::Node {
    k8s::Node {
        metadata: k8s::ObjectMeta {
            name: Some(name.into()),
            ..Default::default()
        },
        spec: Some(k8s::api::core::v1::NodeSpec {
            pod_cidr: Some(pod_net.to_string()),
            pod_cidrs: vec![pod_net.to_string()],
            ..Default::default()
        }),
        status: Some(k8s::api::core::v1::NodeStatus::default()),
    }
}

fn mk_pod(
    ns: impl Into<String>,
    name: impl Into<String>,
    node: impl Into<String>,
    pod_ip: IpAddr,
    containers: impl IntoIterator<Item = (impl Into<String>, impl IntoIterator<Item = u16>)>,
) -> k8s::Pod {
    k8s::Pod {
        metadata: k8s::ObjectMeta {
            namespace: Some(ns.into()),
            name: Some(name.into()),
            ..Default::default()
        },
        spec: Some(k8s::api::core::v1::PodSpec {
            node_name: Some(node.into()),
            containers: containers
                .into_iter()
                .map(|(name, ports)| k8s::api::core::v1::Container {
                    name: name.into(),
                    ports: ports
                        .into_iter()
                        .map(|p| k8s::api::core::v1::ContainerPort {
                            container_port: p as i32,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }),
        status: Some(k8s::api::core::v1::PodStatus {
            pod_ips: vec![k8s::api::core::v1::PodIP {
                ip: Some(pod_ip.to_string()),
            }],
            ..Default::default()
        }),
    }
}

fn mk_server(
    ns: impl Into<String>,
    name: impl Into<String>,
    port: Port,
    srv_labels: impl IntoIterator<Item = (&'static str, &'static str)>,
    pod_labels: impl IntoIterator<Item = (&'static str, &'static str)>,
) -> k8s::polixy::Server {
    k8s::polixy::Server {
        api_version: "v1alpha1".to_string(),
        kind: "Server".to_string(),
        metadata: k8s::ObjectMeta {
            namespace: Some(ns.into()),
            name: Some(name.into()),
            labels: srv_labels
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            ..Default::default()
        },
        spec: k8s::polixy::ServerSpec {
            port,
            pod_selector: pod_labels.into_iter().collect(),
            proxy_protocol: None,
        },
    }
}

fn mk_authz(
    ns: impl Into<String>,
    name: impl Into<String>,
    server: impl Into<String>,
) -> k8s::polixy::ServerAuthorization {
    k8s::polixy::ServerAuthorization {
        api_version: "v1alpha1".to_string(),
        kind: "ServerAuthorization".to_string(),
        metadata: k8s::ObjectMeta {
            namespace: Some(ns.into()),
            name: Some(name.into()),
            ..Default::default()
        },
        spec: k8s::polixy::ServerAuthorizationSpec {
            server: k8s::polixy::authz::Server {
                name: Some(server.into()),
                selector: None,
            },
            client: k8s::polixy::authz::Client {
                // TODO
                ..Default::default()
            },
        },
    }
}

fn mk_default_allow(da: DefaultAllow, cluster_net: IpNet) -> BTreeMap<String, ClientAuthz> {
    let all_nets = Arc::new([
        ClientNetwork {
            net: Ipv4Net::default().into(),
            except: vec![],
        },
        ClientNetwork {
            net: Ipv6Net::default().into(),
            except: vec![],
        },
    ]);

    let cluster_nets = Arc::new([ClientNetwork {
        net: cluster_net,
        except: vec![],
    }]);

    let authed = ClientAuthn::TlsAuthenticated {
        identities: vec![Identity::Suffix(vec![].into())],
        service_accounts: vec![],
    };

    match da {
        DefaultAllow::Deny => None,
        DefaultAllow::AllAuthenticated => Some((
            "_all_authed".into(),
            ClientAuthz {
                authentication: authed.clone(),
                networks: all_nets.clone(),
            },
        )),
        DefaultAllow::AllUnauthenticated => Some((
            "_all_unauthed".into(),
            ClientAuthz {
                authentication: ClientAuthn::Unauthenticated,
                networks: all_nets,
            },
        )),
        DefaultAllow::ClusterAuthenticated => Some((
            "_cluster_authed".into(),
            ClientAuthz {
                authentication: authed,
                networks: cluster_nets.clone(),
            },
        )),
        DefaultAllow::ClusterUnauthenticated => Some((
            "_cluster_unauthed".into(),
            ClientAuthz {
                authentication: ClientAuthn::Unauthenticated,
                networks: cluster_nets,
            },
        )),
    }
    .into_iter()
    .collect()
}
