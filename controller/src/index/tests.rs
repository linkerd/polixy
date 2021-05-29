use super::*;
use crate::*;
use ipnet::{IpNet, Ipv4Net, Ipv6Net};
use std::{str::FromStr, sync::Arc};
use tokio::time;

/// Tests that pod servers are configured with defaults based on the `DefaultAllow` policy
#[tokio::test]
async fn pod_without_server() {
    let cluster_net = IpNet::from_str("192.0.2.0/24").unwrap();
    let pod_net = IpNet::from_str("192.0.2.2/28").unwrap();
    let (kubelet_ip, pod_ip) = {
        let mut ips = pod_net.hosts();
        (ips.next().unwrap(), ips.next().unwrap())
    };
    let detect_timeout = time::Duration::from_secs(1);

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

    let cases = vec![
        (DefaultAllow::Deny, Default::default()),
        (
            DefaultAllow::AllAuthenticated,
            Some((
                "_all_authed".into(),
                ClientAuthz {
                    authentication: authed.clone(),
                    networks: all_nets.clone(),
                },
            ))
            .into_iter()
            .collect::<BTreeMap<_, _>>(),
        ),
        (
            DefaultAllow::AllUnauthenticated,
            Some((
                "_all_unauthed".into(),
                ClientAuthz {
                    authentication: ClientAuthn::Unauthenticated,
                    networks: all_nets,
                },
            ))
            .into_iter()
            .collect::<BTreeMap<_, _>>(),
        ),
        (
            DefaultAllow::ClusterAuthenticated,
            Some((
                "_cluster_authed".into(),
                ClientAuthz {
                    authentication: authed,
                    networks: cluster_nets.clone(),
                },
            ))
            .into_iter()
            .collect::<BTreeMap<_, _>>(),
        ),
        (
            DefaultAllow::ClusterUnauthenticated,
            Some((
                "_cluster_unauthed".into(),
                ClientAuthz {
                    authentication: ClientAuthn::Unauthenticated,
                    networks: cluster_nets,
                },
            ))
            .into_iter()
            .collect::<BTreeMap<_, _>>(),
        ),
    ];

    for (default, authz) in cases.into_iter() {
        let mut idx = Index::new(vec![cluster_net], default, detect_timeout);
        let (mut lookup_tx, lookup_rx) = crate::lookup::pair();

        idx.apply_node(node("node-0", pod_net)).unwrap();

        let p = pod(
            "ns-0",
            "pod-0",
            "node-0",
            pod_ip,
            Some(("container-0", vec![8000, 9000])),
        );
        idx.apply_pod(p, &mut lookup_tx).unwrap();

        let deny_config = InboundServerConfig {
            authorizations: authz,
            protocol: crate::ProxyProtocol::Detect {
                timeout: detect_timeout,
            },
        };

        let port8000 = lookup_rx.lookup("ns-0", "pod-0", 8000).unwrap();
        assert_eq!(port8000.pod_ips, PodIps(Arc::new([pod_ip])));
        assert_eq!(port8000.kubelet_ips, KubeletIps(Arc::new([kubelet_ip])));

        let port9000 = lookup_rx.lookup("ns-0", "pod-0", 9000).unwrap();
        assert!(Arc::ptr_eq(&port9000.pod_ips.0, &port8000.pod_ips.0));
        assert!(Arc::ptr_eq(
            &port9000.kubelet_ips.0,
            &port8000.kubelet_ips.0
        ));

        assert_eq!(*port8000.rx.borrow().borrow(), deny_config);
        assert_eq!(*port9000.rx.borrow().borrow(), deny_config);

        assert!(lookup_rx.lookup("ns-0", "pod-0", 9999).is_none());
    }
}

/// Tests observing a pod before its node has been observed.
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

    let p = pod(
        "ns-0",
        "pod-0",
        "node-0",
        pod_ip,
        Some(("container-0", vec![8000, 9000])),
    );
    let _panics = idx.apply_pod(p, &mut lookup_tx);
}

fn node(name: impl Into<String>, pod_net: IpNet) -> k8s::Node {
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

fn pod(
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
