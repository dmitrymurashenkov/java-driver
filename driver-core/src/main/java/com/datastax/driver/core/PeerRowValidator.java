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

class PeerRowValidator {
    private static final Logger logger = LoggerFactory.getLogger(PeerRowValidator.class);

    private final boolean extendedPeerCheck;

    PeerRowValidator() {
        this(SystemProperties.getBoolean("com.datastax.driver.EXTENDED_PEER_CHECK", true));
    }

    PeerRowValidator(boolean extendedPeerCheck) {
        this.extendedPeerCheck = extendedPeerCheck;
    }

    boolean isValidPeer(Row peerRow, boolean logIfInvalid) {
        boolean isValid = peerRow.getColumnDefinitions().contains("rpc_address")
                && !peerRow.isNull("rpc_address");
        if (extendedPeerCheck) {
            isValid &= peerRow.getColumnDefinitions().contains("host_id")
                    && !peerRow.isNull("host_id")
                    && peerRow.getColumnDefinitions().contains("data_center")
                    && !peerRow.isNull("data_center")
                    && peerRow.getColumnDefinitions().contains("rack")
                    && !peerRow.isNull("rack")
                    && peerRow.getColumnDefinitions().contains("tokens")
                    && !peerRow.isNull("tokens");
        }
        if (!isValid && logIfInvalid)
            logger.warn("Found invalid row in system.peers: {}. " +
                    "This is likely a gossip or snitch issue, this host will be ignored.", formatInvalidPeer(peerRow));
        return isValid;
    }

    // Custom formatting to avoid spamming the logs if 'tokens' is present and contains a gazillion tokens
    private String formatInvalidPeer(Row peerRow) {
        StringBuilder sb = new StringBuilder("[peer=" + peerRow.getInet("peer"));
        formatMissingOrNullColumn(peerRow, "rpc_address", sb);
        if (extendedPeerCheck) {
            formatMissingOrNullColumn(peerRow, "host_id", sb);
            formatMissingOrNullColumn(peerRow, "data_center", sb);
            formatMissingOrNullColumn(peerRow, "rack", sb);
            formatMissingOrNullColumn(peerRow, "tokens", sb);
        }
        sb.append("]");
        return sb.toString();
    }

    private void formatMissingOrNullColumn(Row peerRow, String columnName, StringBuilder sb) {
        if (!peerRow.getColumnDefinitions().contains(columnName))
            sb.append(", missing ").append(columnName);
        else if (peerRow.isNull(columnName))
            sb.append(", ").append(columnName).append("=null");
    }
}
