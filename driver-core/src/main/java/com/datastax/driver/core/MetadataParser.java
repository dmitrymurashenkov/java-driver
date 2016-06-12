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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class MetadataParser {

    private static final Logger logger = LoggerFactory.getLogger(MetadataParser.class);

    private static final InetAddress bindAllAddress;

    static {
        try {
            bindAllAddress = InetAddress.getByAddress(new byte[4]);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    //todo add interface for need Cluster methods
    private final Cluster.Manager cluster;
    private final ClusterHosts hosts;

    public MetadataParser(ClusterHosts hosts, Cluster.Manager cluster) {
        this.hosts = hosts;
        this.cluster = cluster;
    }

    ClusterInfo parseClusterInfo(Row row) throws ExecutionException, InterruptedException {
        return new ClusterInfo(
            row.getString("cluster_name"),
            row.getString("partitioner")
        );
    }

    HostInfo parseHost(Row row, InetSocketAddress address) {
        // Before CASSANDRA-9436 local row did not contain any info about the host addresses.
        // After CASSANDRA-9436 (2.0.16, 2.1.6, 2.2.0 rc1) local row contains two new columns:
        // - broadcast_address
        // - rpc_address
        // After CASSANDRA-9603 (2.0.17, 2.1.8, 2.2.0 rc2) local row contains one more column:
        // - listen_address
        InetAddress broadcastAddress = null;
        if (row.getColumnDefinitions().contains("peer")) { // system.peers
            broadcastAddress = row.getInet("peer");
        } else if (row.getColumnDefinitions().contains("broadcast_address")) { // system.local
            broadcastAddress = row.getInet("broadcast_address");
        }

        // in system.local only for C* versions >= 2.0.17, 2.1.8, 2.2.0 rc2,
        // not yet in system.peers as of C* 3.2
        InetAddress listenAddress = row.getColumnDefinitions().contains("listen_address")
                ? row.getInet("listen_address")
                : null;

        String dseWorkload = null;
        boolean isDseGraph = false;
        String dseVersion = null;
        if (row.getColumnDefinitions().contains("workload")) {
            dseWorkload = row.getString("workload");
        }
        if (row.getColumnDefinitions().contains("graph")) {
            isDseGraph = row.getBool("graph");
        }
        if (row.getColumnDefinitions().contains("dse_version")) {
            dseVersion = row.getString("dse_version");
        }

        return new HostInfo(
                address,
                row.getString("release_version"),
                row.getString("data_center"),
                row.getString("rack"),
                broadcastAddress,
                listenAddress,
                dseWorkload,
                isDseGraph,
                dseVersion,
                row.getSet("tokens", String.class)
        );
    }

    InetSocketAddress resolveHostAddress(Row peersRow, InetSocketAddress connectedHost) {

        // after CASSANDRA-9436, system.peers contains the following inet columns:
        // - peer: this is actually broadcast_address
        // - rpc_address: the address we are looking for (this corresponds to broadcast_rpc_address in the peer's cassandra yaml file;
        //                if this setting if unset, it defaults to the value for rpc_address or rpc_interface)
        // - preferred_ip: used by Ec2MultiRegionSnitch and GossipingPropertyFileSnitch, possibly others; contents unclear

        InetAddress broadcastAddress = peersRow.getInet("peer");
        InetAddress rpcAddress = peersRow.getInet("rpc_address");

        if (broadcastAddress == null) {
            return null;
        } else if (broadcastAddress.equals(connectedHost.getAddress()) || rpcAddress.equals(connectedHost.getAddress())) {
            // Some DSE versions were inserting a line for the local node in peers (with mostly null values). This has been fixed, but if we
            // detect that's the case, ignore it as it's not really a big deal.
            logger.debug("System.peers on node {} has a line for itself. This is not normal but is a known problem of some DSE version. Ignoring the entry.", connectedHost);
            return null;
        } else if (rpcAddress.equals(bindAllAddress)) {
            logger.warn("Found host with 0.0.0.0 as rpc_address, using broadcast_address ({}) to contact it instead. If this is incorrect you should avoid the use of 0.0.0.0 server side.", broadcastAddress);
            rpcAddress = broadcastAddress;
        }
        return cluster.translateAddress(rpcAddress);
    }
}
