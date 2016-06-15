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

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;

interface SystemTablesQuery {

    InetSocketAddress getConnectedHostAddress();
    Row getLocalRow() throws ExecutionException, InterruptedException;
    Iterable<Row> getPeerRows() throws ExecutionException, InterruptedException;

    interface Factory {
        SystemTablesQuery request(Connection connection, ProtocolVersion protocolVersion);
    }

    static final Factory FACTORY = new Factory() {
        @Override
        public SystemTablesQuery request(final Connection connection, ProtocolVersion protocolVersion) {
            final DefaultResultSetFuture localFuture = new DefaultResultSetFuture(null, protocolVersion, new Requests.Query("SELECT * FROM system.local WHERE key='local'"));
            final DefaultResultSetFuture peersFuture = new DefaultResultSetFuture(null, protocolVersion, new Requests.Query("SELECT * FROM system.peers"));
            connection.write(localFuture);
            connection.write(peersFuture);
            return new SystemTablesQuery() {
                @Override
                public InetSocketAddress getConnectedHostAddress() {
                    return connection.address;
                }

                @Override
                public Row getLocalRow() throws ExecutionException, InterruptedException {
                    return localFuture.get().one();
                }

                @Override
                public Iterable<Row> getPeerRows() throws ExecutionException, InterruptedException {
                    return peersFuture.get();
                }
            };
        }
    };
}
