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
package com.datastax.driver.core.policies;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Statement;

import java.util.Collection;
import java.util.Iterator;

class LoadBalancingPolicyWrapper implements LoadBalancingPolicy {
    private final LoadBalancingPolicy delegate;
    private volatile boolean isInit;

    LoadBalancingPolicyWrapper(LoadBalancingPolicy delegate) {
        this.delegate = delegate;
    }

    @Override
    public void init(Cluster cluster, Collection<Host> hosts) {
        delegate.init(cluster, hosts);
        isInit = true;
    }

    @Override
    public HostDistance distance(Host host) {
        if (isInit) {
            return delegate.distance(host);
        } else {
            throw new IllegalStateException("Policy not yet initialized");
        }
    }

    @Override
    public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement) {
        if (isInit) {
            return delegate.newQueryPlan(loggedKeyspace, statement);
        } else {
            throw new IllegalStateException("Policy not yet initialized");
        }
    }

    @Override
    public void onAdd(Host host) {
        if (isInit) {
            delegate.onAdd(host);
        }
    }

    @Override
    public void onUp(Host host) {
        if (isInit) {
            delegate.onUp(host);
        }
    }

    @Override
    public void onDown(Host host) {
        if (isInit) {
            delegate.onDown(host);
        }
    }

    @Override
    public void onRemove(Host host) {
        if (isInit) {
            delegate.onRemove(host);
        }
    }

    @Override
    public void close() {
        if (isInit) {
            delegate.close();
        }
    }
}
