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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Column definitions are taken from Cassandra 3.5. Implement this class for other versions for extensive testing.
 */
class MockSystemTables implements SystemTablesQuery.Factory {

    public static final int PORT = 1234;

    private static final ColumnDefinitions.Definition[] TABLE_LOCAL_COLUMNS = new ColumnDefinitions.Definition[18];
    private static final ColumnDefinitions.Definition[] TABLE_PEERS_COLUMNS = new ColumnDefinitions.Definition[9];

    private MockNodeRow localRow = new MockNodeRow(new ColumnDefinitions(TABLE_LOCAL_COLUMNS, CodecRegistry.DEFAULT_INSTANCE), new InetSocketAddress(TestUtils.addressOfNode(1), PORT));
    private Set<MockNodeRow> peerRows = new LinkedHashSet<MockNodeRow>();


    public MockNodeRow localRow(InetAddress address) {
        localRow = new MockNodeRow(new ColumnDefinitions(TABLE_LOCAL_COLUMNS, CodecRegistry.DEFAULT_INSTANCE), new InetSocketAddress(address, PORT))
                .setColumnValue("cluster_name", "test_cluster")
                .setColumnValue("partitioner", "org.apache.cassandra.dht.Murmur3Partitioner")
                .setColumnValue("data_center", "DC1")
                .setColumnValue("rack", "rack1")
                .setColumnValue("broadcast_address", address)
                .setColumnValue("listen_address", address)
                .setColumnValue("release_version", "3.5")
                .setColumnValue("tokens", new HashSet<String>(Arrays.asList("0")));
        return localRow;
    }

    public MockNodeRow addPeerRow(InetAddress address) {
        MockNodeRow row = new MockNodeRow(new ColumnDefinitions(TABLE_PEERS_COLUMNS, CodecRegistry.DEFAULT_INSTANCE), new InetSocketAddress(address, PORT))
                .setColumnValue("data_center", "DC1")
                .setColumnValue("rack", "rack1")
                .setColumnValue("host_id", UUID.randomUUID())
                .setColumnValue("rpc_address", address)
                .setColumnValue("peer", address)
                .setColumnValue("release_version", "3.5")
                .setColumnValue("tokens", new HashSet<String>(Arrays.asList("5")));
        peerRows.add(row);
        return row;
    }

    public void removePeerRow(MockNodeRow row) {
        peerRows.remove(row);
    }

    public void setPeers(MockNodeRow... rows) {
        peerRows = new HashSet<MockNodeRow>(Arrays.asList(rows));
    }

    @Override
    public SystemTablesQuery request(final Connection connection, ProtocolVersion protocolVersion) {
        return request();
    }

    public SystemTablesQuery request() {
        final InetSocketAddress nodeAddress = localRow.getAddress();
        return new SystemTablesQuery() {
            @Override
            public InetSocketAddress getConnectedHostAddress() {
                return nodeAddress;
            }

            @Override
            public Row getLocalRow() throws ExecutionException, InterruptedException {
                return localRow;
            }

            @Override
            public Iterable<Row> getPeerRows() throws ExecutionException, InterruptedException {
                return (Iterable) Collections.unmodifiableSet(peerRows);
            }
        };
    }

    static {
        TABLE_LOCAL_COLUMNS[0] = new ColumnDefinitions.Definition("system", "local", "key", DataType.varchar());
        TABLE_LOCAL_COLUMNS[1] = new ColumnDefinitions.Definition("system", "local", "bootstrapped", DataType.varchar());
        TABLE_LOCAL_COLUMNS[2] = new ColumnDefinitions.Definition("system", "local", "broadcast_address", DataType.inet());
        TABLE_LOCAL_COLUMNS[3] = new ColumnDefinitions.Definition("system", "local", "cluster_name", DataType.varchar());
        TABLE_LOCAL_COLUMNS[4] = new ColumnDefinitions.Definition("system", "local", "cql_version", DataType.varchar());
        TABLE_LOCAL_COLUMNS[5] = new ColumnDefinitions.Definition("system", "local", "data_center", DataType.varchar());
        TABLE_LOCAL_COLUMNS[6] = new ColumnDefinitions.Definition("system", "local", "gossip_generation", DataType.cint());
        TABLE_LOCAL_COLUMNS[7] = new ColumnDefinitions.Definition("system", "local", "host_id", DataType.uuid());
        TABLE_LOCAL_COLUMNS[8] = new ColumnDefinitions.Definition("system", "local", "listen_address", DataType.inet());
        TABLE_LOCAL_COLUMNS[9] = new ColumnDefinitions.Definition("system", "local", "native_local_version", DataType.varchar());
        TABLE_LOCAL_COLUMNS[10] = new ColumnDefinitions.Definition("system", "local", "partitioner", DataType.varchar());
        TABLE_LOCAL_COLUMNS[11] = new ColumnDefinitions.Definition("system", "local", "rack", DataType.varchar());
        TABLE_LOCAL_COLUMNS[12] = new ColumnDefinitions.Definition("system", "local", "release_version", DataType.varchar());
        TABLE_LOCAL_COLUMNS[13] = new ColumnDefinitions.Definition("system", "local", "rpc_address", DataType.inet());
        TABLE_LOCAL_COLUMNS[14] = new ColumnDefinitions.Definition("system", "local", "schema_version", DataType.uuid());
        TABLE_LOCAL_COLUMNS[15] = new ColumnDefinitions.Definition("system", "local", "thrift_version", DataType.varchar());
        TABLE_LOCAL_COLUMNS[16] = new ColumnDefinitions.Definition("system", "local", "tokens", DataType.set(DataType.varchar()));
        TABLE_LOCAL_COLUMNS[17] = new ColumnDefinitions.Definition("system", "local", "truncated_at", DataType.map(DataType.uuid(), DataType.blob()));

        TABLE_PEERS_COLUMNS[0] = new ColumnDefinitions.Definition("system", "peers", "peer", DataType.inet());
        TABLE_PEERS_COLUMNS[1] = new ColumnDefinitions.Definition("system", "peers", "data_center", DataType.varchar());
        TABLE_PEERS_COLUMNS[2] = new ColumnDefinitions.Definition("system", "peers", "host_id", DataType.uuid());
        TABLE_PEERS_COLUMNS[3] = new ColumnDefinitions.Definition("system", "peers", "preferred_ip", DataType.inet());
        TABLE_PEERS_COLUMNS[4] = new ColumnDefinitions.Definition("system", "peers", "rack", DataType.varchar());
        TABLE_PEERS_COLUMNS[5] = new ColumnDefinitions.Definition("system", "peers", "release_version", DataType.varchar());
        TABLE_PEERS_COLUMNS[6] = new ColumnDefinitions.Definition("system", "peers", "rpc_address", DataType.inet());
        TABLE_PEERS_COLUMNS[7] = new ColumnDefinitions.Definition("system", "peers", "schema_version", DataType.uuid());
        TABLE_PEERS_COLUMNS[8] = new ColumnDefinitions.Definition("system", "peers", "tokens", DataType.set(DataType.text()));
        //todo add all columns
//        TABLE_PEERS_COLUMNS[3] = new ColumnDefinitions.Definition("system", "peers", "listen_address", DataType.inet());
//        TABLE_PEERS_COLUMNS[8] = new ColumnDefinitions.Definition("system", "peers", "graph", DataType.cboolean());
    }


}
