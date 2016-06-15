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

import com.google.common.collect.Sets;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static com.datastax.driver.core.TestUtils.addressOfNode;
import static org.testng.Assert.*;

public class FetchClusterMetadataOperationUnitTest {

    @Test(groups = "unit")
    public void getClusterInfo_should_return_cluster_info() throws ClusterNameMismatchException, InterruptedException, ExecutionException {

        MockSystemTables systemTables = new MockSystemTables();
        systemTables.localRow(addressOfNode(1))
                .setColumnValue("cluster_name", "scassandra")
                .setColumnValue("partitioner", "org.apache.cassandra.dht.Murmur3Partitioner");
        FetchClusterMetadataOperation fetch = new FetchClusterMetadataOperation(systemTables.request(), new ClusterMetadataParser(), new MockRpcPortResolver(), new PeerRowValidator());
        fetch.reloadData(true);
        ClusterInfo clusterInfo = fetch.getClusterInfo();

        assertNotNull(clusterInfo);
        assertEquals(clusterInfo.clusterName, "scassandra");
        assertEquals(clusterInfo.partitioner, "org.apache.cassandra.dht.Murmur3Partitioner");
    }

    @Test(groups = "unit")
    public void getHosts_should_return_info_about_connected_host() throws ClusterNameMismatchException, InterruptedException, ExecutionException {
        MockSystemTables systemTables = new MockSystemTables();
        MockNodeRow localRow = systemTables.localRow(addressOfNode(1))
                .setColumnValue("release_version", "2.1.8")
                .setColumnValue("data_center", "DC1")
                .setColumnValue("rack", "rack1")
                .setColumnValue("broadcast_address", addressOfNode(1))
                .setColumnValue("listen_address", addressOfNode(1))
                .setColumnValue("tokens", new HashSet<String>(Arrays.asList("0")));
        FetchClusterMetadataOperation fetch = new FetchClusterMetadataOperation(systemTables.request(), new ClusterMetadataParser(), new MockRpcPortResolver(), new PeerRowValidator());
        fetch.reloadData(true);
        List<HostInfo> hosts = fetch.getHostsInfo();

        assertEquals(hosts.size(), 1);
        HostInfo host = hosts.get(0);
        assertEquals(host.address, localRow.getAddress());
        assertEquals(host.version, "2.1.8");
        assertEquals(host.datacenter, "DC1");
        assertEquals(host.rack, "rack1");
        assertEquals(host.broadcastAddress, addressOfNode(1));
        assertEquals(host.listenAddress, addressOfNode(1));
        assertNull(host.dseWorkload);
        assertFalse(host.isDseGraphEnabled);
        assertNull(host.dseVersion);
        assertEquals(host.tokens, Sets.newHashSet("0"));
    }

    @Test(groups = "unit")
    public void getHosts_should_return_info_about_peer_host() throws ClusterNameMismatchException, InterruptedException, ExecutionException {
        MockSystemTables systemTables = new MockSystemTables();
        MockNodeRow localRow = systemTables.localRow(addressOfNode(1))
                .setColumnValue("release_version", "2.1.8")
                .setColumnValue("data_center", "DC1")
                .setColumnValue("rack", "rack1")
                .setColumnValue("broadcast_address", addressOfNode(1))
                .setColumnValue("listen_address", addressOfNode(1))
                .setColumnValue("tokens", new HashSet<String>(Arrays.asList("0")));
        MockNodeRow peerRow = systemTables.addPeerRow(addressOfNode(2))
                .setColumnValue("release_version", "2.1.8")
                .setColumnValue("data_center", "DC1")
                .setColumnValue("rack", "rack2")
                .setColumnValue("host_id", UUID.randomUUID())
                .setColumnValue("tokens", new HashSet<String>(Arrays.asList("4611686018427387903")));
        FetchClusterMetadataOperation fetch = new FetchClusterMetadataOperation(systemTables.request(), new ClusterMetadataParser(), new MockRpcPortResolver(), new PeerRowValidator());
        fetch.reloadData(true);
        List<HostInfo> hosts = fetch.getHostsInfo();

        //first host is always connected one - lets keep it that way for simplicity
        assertEquals(hosts.size(), 2);

        HostInfo connectedHost = hosts.get(0);
        assertEquals(connectedHost.address, localRow.getAddress());
        assertEquals(connectedHost.version, "2.1.8");
        assertEquals(connectedHost.datacenter, "DC1");
        assertEquals(connectedHost.rack, "rack1");
        assertEquals(connectedHost.broadcastAddress, addressOfNode(1));
        assertEquals(connectedHost.listenAddress, addressOfNode(1));
        assertNull(connectedHost.dseWorkload);
        assertFalse(connectedHost.isDseGraphEnabled);
        assertNull(connectedHost.dseVersion);
        assertEquals(connectedHost.tokens, Sets.newHashSet("0"));

        HostInfo peerHost = hosts.get(1);
        assertEquals(peerHost.address, peerRow.getAddress());
        assertEquals(peerHost.version, "2.1.8");
        assertEquals(peerHost.datacenter, "DC1");
        assertEquals(peerHost.rack, "rack2");
        assertEquals(peerHost.broadcastAddress, addressOfNode(2));
        assertNull(peerHost.listenAddress);
        assertNull(peerHost.dseWorkload);
        assertFalse(peerHost.isDseGraphEnabled);
        assertNull(peerHost.dseVersion);
        assertEquals(peerHost.tokens, Sets.newHashSet("4611686018427387903"));
    }

    @Test(groups = "unit")
    public void getHosts_should_ignore_peer_with_nulls_in_required_columns() throws ClusterNameMismatchException, InterruptedException, ExecutionException {
        List<String> nonNullColumns = Arrays.asList(
                "rpc_address",
                "host_id",
                "data_center",
                "rack",
                "tokens");
        for (String nonNullColumn : nonNullColumns) {
            MockSystemTables systemTables = new MockSystemTables();
            MockNodeRow node1 = systemTables.localRow(addressOfNode(1));
            MockNodeRow node2 = systemTables.addPeerRow(addressOfNode(2)).setColumnValue(nonNullColumn, null);
            MockNodeRow node3 = systemTables.addPeerRow(addressOfNode(3));
            FetchClusterMetadataOperation fetch = new FetchClusterMetadataOperation(systemTables.request(), new ClusterMetadataParser(), new MockRpcPortResolver(), new PeerRowValidator());
            fetch.reloadData(true);

            List<HostInfo> hosts = fetch.getHostsInfo();
            assertEquals(hosts.size(), 2);
            assertEquals(hosts.get(0).address, node1.getAddress());
            assertEquals(hosts.get(1).address, node3.getAddress());
        }
    }

    @Test(groups = "unit")
    public void getHosts_should_ignore_peer_with_nulls_in_critical_required_columns_if_extended_peer_check_false() throws ClusterNameMismatchException, InterruptedException, ExecutionException {
        List<String> requiredColumns = Arrays.asList(
                "rpc_address");
        for (String requiredColumn : requiredColumns) {
            MockSystemTables systemTables = new MockSystemTables();
            MockNodeRow node1 = systemTables.localRow(addressOfNode(1));
            MockNodeRow node2 = systemTables.addPeerRow(addressOfNode(2)).setColumnValue(requiredColumn, null);
            MockNodeRow node3 = systemTables.addPeerRow(addressOfNode(3));
            FetchClusterMetadataOperation fetch = new FetchClusterMetadataOperation(systemTables.request(), new ClusterMetadataParser(), new MockRpcPortResolver(), new PeerRowValidator(false));
            fetch.reloadData(true);

            List<HostInfo> hosts = fetch.getHostsInfo();
            assertEquals(hosts.size(), 2);
            assertEquals(hosts.get(0).address, node1.getAddress());
            assertEquals(hosts.get(1).address, node3.getAddress());
        }
    }

    @Test(groups = "unit")
    public void getHosts_should_add_peer_with_nulls_in_non_critical_columns_if_extended_peer_check_false() throws ClusterNameMismatchException, InterruptedException, ExecutionException {
        List<String> nonCriticalRequiredColumns = Arrays.asList(
                "host_id",
                "data_center",
                "rack",
                "tokens");
        for (String nonNullColumn : nonCriticalRequiredColumns) {
            MockSystemTables systemTables = new MockSystemTables();
            MockNodeRow node1 = systemTables.localRow(addressOfNode(1));
            MockNodeRow node2 = systemTables.addPeerRow(addressOfNode(2)).setColumnValue(nonNullColumn, null);
            MockNodeRow node3 = systemTables.addPeerRow(addressOfNode(3));
            FetchClusterMetadataOperation fetch = new FetchClusterMetadataOperation(systemTables.request(), new ClusterMetadataParser(), new MockRpcPortResolver(), new PeerRowValidator(false));
            fetch.reloadData(true);

            List<HostInfo> hosts = fetch.getHostsInfo();
            assertEquals(hosts.size(), 3);
            assertEquals(hosts.get(0).address, node1.getAddress());
            assertEquals(hosts.get(1).address, node2.getAddress());
            assertEquals(hosts.get(2).address, node3.getAddress());
        }
    }

    @Test(groups = "unit")
    public void getHosts_should_add_peer_if_rpc_address_is_bind_all() throws UnknownHostException, ClusterNameMismatchException, InterruptedException, ExecutionException {
        MockSystemTables systemTables = new MockSystemTables();
        MockNodeRow node1 = systemTables.localRow(addressOfNode(1));
        MockNodeRow node2 = systemTables.addPeerRow(addressOfNode(2)).setColumnValue("rpc_address", InetAddress.getByAddress(new byte[4]));
        MockNodeRow node3 = systemTables.addPeerRow(addressOfNode(3));
        FetchClusterMetadataOperation fetch = new FetchClusterMetadataOperation(systemTables.request(), new ClusterMetadataParser(), new MockRpcPortResolver(), new PeerRowValidator(false));
        fetch.reloadData(true);

        List<HostInfo> hosts = fetch.getHostsInfo();
        assertEquals(hosts.size(), 3);
        assertEquals(hosts.get(0).address, node1.getAddress());
        assertEquals(hosts.get(1).address, node2.getAddress());
        assertEquals(hosts.get(2).address, node3.getAddress());
    }

    static class MockRpcPortResolver implements RpcPortResolver {
        @Override
        public InetSocketAddress getRpcAddress(InetAddress connectedHost) {
            return new InetSocketAddress(connectedHost, MockSystemTables.PORT);
        }
    }
}
