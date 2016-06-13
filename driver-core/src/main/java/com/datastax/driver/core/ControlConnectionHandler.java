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

import com.datastax.driver.core.exceptions.DriverException;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

class ControlConnectionHandler {
    private static final String SELECT_PEERS = "SELECT * FROM system.peers";
    private static final String SELECT_LOCAL = "SELECT * FROM system.local WHERE key='local'";

    private final Connection connection;
    private final ProtocolVersion protocolVersion;
    private final MetadataParser metadataParser;
    private final PeerRowValidator rowValidator;

    private volatile ClusterInfo clusterInfo;
    private volatile List<HostInfo> hosts;

    ControlConnectionHandler(Connection connection, ProtocolVersion protocolVersion, MetadataParser metadataParser, PeerRowValidator rowValidator) {
        this.connection = connection;
        this.protocolVersion = protocolVersion;
        this.metadataParser = metadataParser;
        this.rowValidator = rowValidator;
    }

    void reloadData(boolean logInvalidPeers) {
        try {
            DefaultResultSetFuture localFuture = new DefaultResultSetFuture(null, protocolVersion, new Requests.Query(SELECT_LOCAL));
            DefaultResultSetFuture peersFuture = new DefaultResultSetFuture(null, protocolVersion, new Requests.Query(SELECT_PEERS));
            connection.write(localFuture);
            connection.write(peersFuture);

            List<HostInfo> hosts = new ArrayList<HostInfo>();
            Row localNodeRow = localFuture.get().one();
            if (localNodeRow != null) {
                clusterInfo = metadataParser.parseClusterInfo(localNodeRow);
                hosts.add(metadataParser.parseHost(localNodeRow, connection.address));
            }

            for (Row peerNodeRow : peersFuture.get()) {
                if (rowValidator.isValidPeer(peerNodeRow, logInvalidPeers)) {
                    InetSocketAddress peerAddress = metadataParser.resolveHostAddress(peerNodeRow, connection.address);
                    if (peerAddress != null) {
                        hosts.add(metadataParser.parseHost(peerNodeRow, peerAddress));
                    }
                }
            }
            this.hosts = Collections.unmodifiableList(hosts);
        }
        catch (InterruptedException e) {
            //todo exception handling
            throw new DriverException(e);
        } catch (ExecutionException e) {
            //todo exception handling
            throw new DriverException(e);
        }
    }

    ClusterInfo getClusterInfo() {
        return clusterInfo;
    }

    List<HostInfo> getHostsInfo() {
        return hosts;
    }
}
