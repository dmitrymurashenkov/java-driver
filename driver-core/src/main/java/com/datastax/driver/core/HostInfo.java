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
import java.util.Set;

class HostInfo {
    final InetSocketAddress address;

    final String version;

    final String datacenter;
    final String rack;

    final InetAddress broadcastAddress;
    final InetAddress listenAddress;

    final String dseWorkload;
    final boolean isDseGraphEnabled;
    final String dseVersion;

    final Set<String> tokens;

    HostInfo(
            InetSocketAddress address,
            String version,
            String datacenter,
            String rack,
            InetAddress broadcastAddress,
            InetAddress listenAddress,
            String dseWorkload,
            boolean isDseGraphEnabled,
            String dseVersion,
            Set<String> tokens) {
        this.address = address;
        this.version = version;
        this.datacenter = datacenter;
        this.rack = rack;
        this.broadcastAddress = broadcastAddress;
        this.listenAddress = listenAddress;
        this.dseWorkload = dseWorkload;
        this.isDseGraphEnabled = isDseGraphEnabled;
        this.dseVersion = dseVersion;
        this.tokens = tokens;
    }
}