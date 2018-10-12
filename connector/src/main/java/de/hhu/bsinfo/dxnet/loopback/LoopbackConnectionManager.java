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

package de.hhu.bsinfo.dxnet.loopback;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxnet.MessageHandlers;
import de.hhu.bsinfo.dxnet.NodeMap;
import de.hhu.bsinfo.dxnet.core.AbstractConnection;
import de.hhu.bsinfo.dxnet.core.AbstractConnectionManager;
import de.hhu.bsinfo.dxnet.core.AbstractExporterPool;
import de.hhu.bsinfo.dxnet.core.BufferPool;
import de.hhu.bsinfo.dxnet.core.CoreConfig;
import de.hhu.bsinfo.dxnet.core.DynamicExporterPool;
import de.hhu.bsinfo.dxnet.core.IncomingBufferQueue;
import de.hhu.bsinfo.dxnet.core.LocalMessageHeaderPool;
import de.hhu.bsinfo.dxnet.core.MessageDirectory;
import de.hhu.bsinfo.dxnet.core.RequestMap;
import de.hhu.bsinfo.dxnet.core.StaticExporterPool;

/**
 * Connection manager for loopback.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 21.09.2017
 */
public class LoopbackConnectionManager extends AbstractConnectionManager {
    private static final Logger LOGGER = LogManager.getFormatterLogger(LoopbackConnectionManager.class.getSimpleName());

    private final CoreConfig m_coreConfig;
    private final LoopbackConfig m_loopbackConfig;

    private final MessageDirectory m_messageDirectory;
    private final RequestMap m_requestMap;
    private final IncomingBufferQueue m_incomingBufferQueue;
    private final LocalMessageHeaderPool m_messageHeaderPool;
    private final MessageHandlers m_messageHandlers;

    private final LoopbackSendThread m_loopbackSendThread;
    private final BufferPool m_bufferPool;
    private final NodeMap m_nodeMap;

    private AbstractExporterPool m_exporterPool;

    public LoopbackConnectionManager(final CoreConfig p_coreConfig, final LoopbackConfig p_nioConfig,
            final NodeMap p_nodeMap, final MessageDirectory p_messageDirectory, final RequestMap p_requestMap,
            final IncomingBufferQueue p_incomingBufferQueue, final LocalMessageHeaderPool p_messageHeaderPool,
            final MessageHandlers p_messageHandlers, final boolean p_overprovisioning) {
        super(2, p_overprovisioning);

        m_coreConfig = p_coreConfig;
        m_loopbackConfig = p_nioConfig;

        m_nodeMap = p_nodeMap;
        m_messageDirectory = p_messageDirectory;
        m_requestMap = p_requestMap;
        m_incomingBufferQueue = p_incomingBufferQueue;
        m_messageHeaderPool = p_messageHeaderPool;
        m_messageHandlers = p_messageHandlers;

        LOGGER.info("Starting LoopbackSendThread...");

        m_bufferPool = new BufferPool((int) m_loopbackConfig.getOutgoingRingBufferSize().getBytes());
        if (m_coreConfig.isUseStaticExporterPool()) {
            m_exporterPool = new StaticExporterPool();
        } else {
            m_exporterPool = new DynamicExporterPool();
        }

        m_loopbackSendThread =
                new LoopbackSendThread(this, (int) p_nioConfig.getConnectionTimeOut().getMs(),
                        (int) m_loopbackConfig.getOutgoingRingBufferSize().getBytes(), p_overprovisioning);
        m_loopbackSendThread.setName("Network-LoopbackSendThread");
        m_loopbackSendThread.start();
    }

    @Override
    public void close() {
        LOGGER.info("LoopbackSendThread close...");
        m_loopbackSendThread.close();
    }

    @Override
    public void setOverprovisioning() {
        super.setOverprovisioning();
        m_loopbackSendThread.activateParking();
    }

    @Override
    public AbstractConnection createConnection(final short p_destination,
            final AbstractConnection p_existingConnection) {
        LoopbackConnection ret;

        ret = new LoopbackConnection(m_coreConfig.getOwnNodeId(), p_destination,
                (int) m_loopbackConfig.getOutgoingRingBufferSize().getBytes(),
                (int) m_loopbackConfig.getFlowControlWindow().getBytes(),
                m_loopbackConfig.getFlowControlWindowThreshold(), m_incomingBufferQueue, m_messageHeaderPool,
                m_messageDirectory, m_requestMap, m_messageHandlers, m_bufferPool, m_exporterPool,
                m_loopbackSendThread, m_nodeMap, m_coreConfig.isBenchmarkMode());

        return ret;
    }

    @Override
    protected void closeConnection(final AbstractConnection p_connection, final boolean p_removeConnection) {
        LoopbackConnection connection = (LoopbackConnection) p_connection;
        connection.setPipeOutConnected(false);
        connection.setPipeInConnected(false);
    }
}
