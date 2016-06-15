/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.google.common.collect.Sets;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.datastax.driver.core.TestUtils.nonQuietClusterCloseOptions;
import static org.testng.Assert.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class FetchClusterMetadataOperationShortTest {
    @Test(groups = "short")
    public void getClusterInfo_should_return_cluster_info() throws ClusterNameMismatchException, InterruptedException, ExecutionException {
        ScassandraCluster scassandra = ScassandraCluster.builder().withNodes(1).build();

        new RunClusterAssertion(scassandra) {
            @Override
            void performAssert(ScassandraCluster scassandra, FetchClusterMetadataOperation connection) {
                ClusterInfo clusterInfo = connection.getClusterInfo();

                assertNotNull(clusterInfo);
                assertEquals(clusterInfo.clusterName, "scassandra");
                assertEquals(clusterInfo.partitioner, "org.apache.cassandra.dht.Murmur3Partitioner");
            }
        };
    }

    @Test(groups = "short")
    public void getHosts_should_return_info_about_connected_host() throws ClusterNameMismatchException, InterruptedException, ExecutionException {
        ScassandraCluster scassandra = ScassandraCluster.builder()
                .withNodes(1)
                .forcePeerInfo(1, 1, "release_version", "2.1.8")
                .build();

        new RunClusterAssertion(scassandra) {
            @Override
            void performAssert(ScassandraCluster scassandra, FetchClusterMetadataOperation connection) {
                List<HostInfo> hosts = connection.getHostsInfo();

                assertEquals(hosts.size(), 1);
                HostInfo host = hosts.get(0);
                assertEquals(host.address, scassandra.address(1));
                assertEquals(host.version, "2.1.8");
                assertEquals(host.datacenter, "DC1");
                assertEquals(host.rack, "rack1");
                assertEquals(host.broadcastAddress, scassandra.address(1).getAddress());
                assertEquals(host.listenAddress, scassandra.address(1).getAddress());
                assertNull(host.dseWorkload);
                assertFalse(host.isDseGraphEnabled);
                assertNull(host.dseVersion);
                assertEquals(host.tokens, Sets.newHashSet("0"));
            }
        };
    }

    @Test(groups = "short")
    public void getHosts_should_return_info_about_peer_host() throws ClusterNameMismatchException, InterruptedException, ExecutionException {
        ScassandraCluster scassandra = ScassandraCluster.builder()
                .withNodes(2)
                .forcePeerInfo(1, 1, "release_version", "2.1.8")
                .forcePeerInfo(1, 2, "release_version", "2.1.8")
                .build();

        new RunClusterAssertion(scassandra) {
            @Override
            void performAssert(ScassandraCluster scassandra, FetchClusterMetadataOperation connection) {
                List<HostInfo> hosts = connection.getHostsInfo();
                //first host is always connected one - lets keep it that way for simplicity
                assertEquals(hosts.size(), 2);

                HostInfo connectedHost = hosts.get(0);
                assertEquals(connectedHost.address, scassandra.address(1));
                assertEquals(connectedHost.version, "2.1.8");
                assertEquals(connectedHost.datacenter, "DC1");
                assertEquals(connectedHost.rack, "rack1");
                assertEquals(connectedHost.broadcastAddress, scassandra.address(1).getAddress());
                assertEquals(connectedHost.listenAddress, scassandra.address(1).getAddress());
                assertNull(connectedHost.dseWorkload);
                assertFalse(connectedHost.isDseGraphEnabled);
                assertNull(connectedHost.dseVersion);
                assertEquals(connectedHost.tokens, Sets.newHashSet("0"));

                HostInfo peerHost = hosts.get(1);
                assertEquals(peerHost.address, scassandra.address(2));
                assertEquals(peerHost.version, "2.1.8");
                assertEquals(peerHost.datacenter, "DC1");
                assertEquals(peerHost.rack, "rack1");
                assertEquals(peerHost.broadcastAddress, scassandra.address(2).getAddress());
                //todo listen address is null in scassandra
                assertNull(peerHost.listenAddress);
                assertNull(peerHost.dseWorkload);
                assertFalse(peerHost.isDseGraphEnabled);
                assertNull(peerHost.dseVersion);
                assertEquals(peerHost.tokens, Sets.newHashSet("4611686018427387903"));
            }
        };
    }

    @Test(groups = "short")
    public void getHosts_should_ignore_peer_with_nulls_in_required_columns() throws ClusterNameMismatchException, InterruptedException, ExecutionException {
        List<String> nonNullColumns = Arrays.asList(
                "peer",
                "rpc_address",
                "host_id",
                "data_center",
                "rack",
                "tokens");
        for (String nonNullColumn : nonNullColumns) {
            ScassandraCluster scassandra = ScassandraCluster.builder()
                    .withNodes(3)
                    .forcePeerInfo(1, 2, nonNullColumn, null)
                    .build();
            new RunClusterAssertion(scassandra) {
                @Override
                void performAssert(ScassandraCluster scassandra, FetchClusterMetadataOperation connection) {
                    List<HostInfo> hosts = connection.getHostsInfo();
                    assertEquals(hosts.size(), 2);
                    assertEquals(hosts.get(0).address, scassandra.address(1));
                    assertEquals(hosts.get(1).address, scassandra.address(3));
                }
            };
        }
    }

    @Test(groups = "short")
    public void getHosts_should_ignore_peer_with_nulls_in_critical_required_columns_if_extended_peer_check_false() throws ClusterNameMismatchException, InterruptedException, ExecutionException {
        List<String> requiredColumns = Arrays.asList(
                "peer",
                "rpc_address");
        for (String requiredColumn : requiredColumns) {
            ScassandraCluster scassandra = ScassandraCluster.builder()
                    .withNodes(3)
                    .forcePeerInfo(1, 2, requiredColumn, null)
                    .build();
            new RunClusterAssertion(scassandra, new PeerRowValidator(false)) {
                @Override
                void performAssert(ScassandraCluster scassandra, FetchClusterMetadataOperation connection) {
                    List<HostInfo> hosts = connection.getHostsInfo();
                    assertEquals(hosts.size(), 2);
                    assertEquals(hosts.get(0).address, scassandra.address(1));
                    assertEquals(hosts.get(1).address, scassandra.address(3));
                }
            };
        }
    }

    @Test(groups = "short")
    public void getHosts_should_add_peer_with_nulls_in_non_critical_columns_if_extended_peer_check_false() throws ClusterNameMismatchException, InterruptedException, ExecutionException {
        List<String> nonCriticalRequiredColumns = Arrays.asList(
                "host_id",
                "data_center",
                "rack",
                "tokens");
        for (String nonNullColumn : nonCriticalRequiredColumns) {
            ScassandraCluster scassandra = ScassandraCluster.builder()
                    .withNodes(3)
                    .forcePeerInfo(1, 2, nonNullColumn, null)
                    .build();
            new RunClusterAssertion(scassandra, new PeerRowValidator(false)) {
                @Override
                void performAssert(ScassandraCluster scassandra, FetchClusterMetadataOperation connection) {
                    List<HostInfo> hosts = connection.getHostsInfo();
                    assertEquals(hosts.size(), 3);
                    assertEquals(hosts.get(0).address, scassandra.address(1));
                    assertEquals(hosts.get(1).address, scassandra.address(2));
                    assertEquals(hosts.get(2).address, scassandra.address(3));
                }
            };
        }
    }

    @Test(groups = "short")
    public void getHosts_should_add_peer_if_rpc_address_is_bind_all() throws UnknownHostException, ClusterNameMismatchException, InterruptedException, ExecutionException {
        ScassandraCluster scassandra = ScassandraCluster.builder()
                .withNodes(3)
                .forcePeerInfo(1, 2, "rpc_address", InetAddress.getByAddress(new byte[4]))
                .build();
        new RunClusterAssertion(scassandra) {
            @Override
            void performAssert(ScassandraCluster scassandra, FetchClusterMetadataOperation connection) {
                List<HostInfo> hosts = connection.getHostsInfo();
                assertEquals(hosts.size(), 3);
                assertEquals(hosts.get(0).address, scassandra.address(1));
                assertEquals(hosts.get(1).address, scassandra.address(2));
                assertEquals(hosts.get(2).address, scassandra.address(3));
            }
        };
    }

    /**
     * Utility class to wrap running/closing cluster into try-finally block
     */
    private abstract static class RunClusterAssertion {
        RunClusterAssertion(ScassandraCluster scassandra) throws ClusterNameMismatchException, InterruptedException, ExecutionException {
            this(scassandra, new PeerRowValidator());
        }

        RunClusterAssertion(ScassandraCluster scassandra, PeerRowValidator rowValidator) throws ClusterNameMismatchException, InterruptedException, ExecutionException {
            //calling run right away so that nobody forgets to invoke it
            run(scassandra, rowValidator);
        }

        private void run(ScassandraCluster scassandra, PeerRowValidator rowValidator) throws ClusterNameMismatchException, InterruptedException, ExecutionException {
            Cluster cluster = Cluster.builder()
                    .addContactPoints(scassandra.address(1).getAddress())
                    .withPort(scassandra.getBinaryPort())
                    .withNettyOptions(nonQuietClusterCloseOptions)
                    .withReconnectionPolicy(new ConstantReconnectionPolicy(3600*1000))
                    .build();

            FetchClusterMetadataOperation connection = null;
            try {
                scassandra.init();
                cluster.init();
                connection = openConnection(scassandra, cluster, rowValidator);
                connection.reloadData(false);
                performAssert(scassandra, connection);
            } finally {
                cluster.close();
                scassandra.stop();
            }
        }

        FetchClusterMetadataOperation openConnection(ScassandraCluster scassandra, Cluster cluster, PeerRowValidator rowValidator) throws ClusterNameMismatchException, InterruptedException {
            Connection connection = cluster.manager.connectionFactory.open(cluster.getMetadata().getHost(scassandra.address(1)));
            return new FetchClusterMetadataOperation(SystemTablesQuery.FACTORY.request(connection, cluster.manager.protocolVersion()), new ClusterMetadataParser(), new RpcPortResolver.Impl(cluster.manager), rowValidator);
        }

        abstract void performAssert(ScassandraCluster scassandra, FetchClusterMetadataOperation connection);
    }
}
