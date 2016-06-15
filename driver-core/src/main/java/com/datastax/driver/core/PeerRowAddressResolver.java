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

class PeerRowAddressResolver {
    private static final Logger logger = LoggerFactory.getLogger(PeerRowAddressResolver.class);

    private static final InetAddress bindAllAddress;

    static {
        try {
            bindAllAddress = InetAddress.getByAddress(new byte[4]);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    private final Cluster.Manager manager;

    PeerRowAddressResolver(Cluster.Manager manager) {
        this.manager = manager;
    }

    InetSocketAddress getRpcAddress(Row peersRow, InetSocketAddress connectedHost) {
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
        return manager.translateAddress(rpcAddress);
    }
}
