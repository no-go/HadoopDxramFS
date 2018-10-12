/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxnet.core;

import lombok.Data;
import lombok.experimental.Accessors;

import com.google.gson.annotations.Expose;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxnet.NetworkDeviceType;
import de.hhu.bsinfo.dxutils.NodeID;

/**
 * Common configuration values for the core parts of the network subsystem
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 28.07.2017
 */
@Data
@Accessors(prefix = "m_")
public class CoreConfig {
    private static final Logger LOGGER = LogManager.getFormatterLogger(CoreConfig.class);

    /**
     * Get the node id of the current node.
     * Don't expose, not a configurable attribute
     */
    private short m_ownNodeId = NodeID.INVALID_ID;

    /**
     * Number of threads to spawn for handling incoming and assembled network messages
     */
    @Expose
    private int m_numMessageHandlerThreads = 2;

    /**
     * Size of the map that stores outstanding requests and maps them to their incoming responses
     */
    @Expose
    private int m_requestMapSize = 1048576;

    /**
     * The exporter pool type. True if static, false if dynamic. Static is recommended for less than 1000
     * actively message sending threads.
     */
    @Expose
    private boolean m_useStaticExporterPool = true;

    /**
     * Set this to true to put DXNet into benchmark mode which enables further statistics like percentile for RTT
     * (note: the application requires more memory in this mode due to recording more data for evaluation purpose)
     */
    @Expose
    private boolean m_benchmarkMode = false;

    /**
     * The device name (Ethernet, Infiniband or Loopback)
     */
    @Expose
    private String m_device = NetworkDeviceType.ETHERNET_STR;

    /**
     * Get the network device
     *
     * @return Network device
     */
    public NetworkDeviceType getDevice() {
        return NetworkDeviceType.toNetworkDeviceType(m_device);
    }

    /**
     * Verify the configuration values
     *
     * @return True if all configuration values are ok, false on invalid value, range or any other error
     */
    public boolean verify() {
        if (NetworkDeviceType.toNetworkDeviceType(m_device) == NetworkDeviceType.INVALID) {
            LOGGER.error("Invalid network device '%s' specified", m_device);
            return false;
        }

        if (m_requestMapSize <= (int) Math.pow(2, 15)) {
            LOGGER.warn("Request map entry count is rather small. Requests might be discarded!");
            return true;
        }

        return true;
    }
}
