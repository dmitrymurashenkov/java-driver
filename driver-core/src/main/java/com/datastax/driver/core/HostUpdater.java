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

import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.google.common.base.Objects;

class HostUpdater {
    private final LoadBalancingPolicy loadBalancingPolicy;

    HostUpdater(LoadBalancingPolicy loadBalancingPolicy) {
        this.loadBalancingPolicy = loadBalancingPolicy;
    }

    void updateHost(Host host, HostInfo hostInfo) {
        updateLocationInfo(host, hostInfo.datacenter, hostInfo.rack);
        host.setVersion(hostInfo.version);
        host.setBroadcastAddress(hostInfo.broadcastAddress);
        host.setListenAddress(hostInfo.listenAddress);
        host.setDseVersion(hostInfo.dseVersion);
        host.setDseWorkload(hostInfo.dseWorkload);
        host.setDseGraphEnabled(hostInfo.isDseGraphEnabled);
    }

    private void updateLocationInfo(Host host, String datacenter, String rack) {
        if (Objects.equal(host.getDatacenter(), datacenter) && Objects.equal(host.getRack(), rack))
            return;

        // If the dc/rack information changes for an existing node, we need to update the load balancing policy.
        // For that, we remove and re-add the node against the policy. Not the most elegant, and assumes
        // that the policy will update correctly, but in practice this should work.
        loadBalancingPolicy.onDown(host);
        host.setLocationInfo(datacenter, rack);
        loadBalancingPolicy.onAdd(host);
    }
}
