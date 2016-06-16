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
import java.net.UnknownHostException;

public class MockNodeRow extends MockRow {
    private InetSocketAddress address;

    MockNodeRow(ColumnDefinitions columnDefinitions, InetSocketAddress address) {
        super(columnDefinitions);
        this.address = address;
        address(address);
    }

    public InetSocketAddress getAddress() {
        return address;
    }

    MockNodeRow address(String host) {
        try {
            return address(new InetSocketAddress(InetAddress.getByName(host), address.getPort()));
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    MockNodeRow address(InetSocketAddress address) {
        this.address = address;
        return this;
    }
}