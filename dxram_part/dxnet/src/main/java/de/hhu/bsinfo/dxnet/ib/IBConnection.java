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

package de.hhu.bsinfo.dxnet.ib;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxnet.MessageHandlers;
import de.hhu.bsinfo.dxnet.core.AbstractConnection;
import de.hhu.bsinfo.dxnet.core.AbstractExporterPool;
import de.hhu.bsinfo.dxnet.core.LocalMessageHeaderPool;
import de.hhu.bsinfo.dxnet.core.MessageDirectory;
import de.hhu.bsinfo.dxnet.core.RequestMap;

/**
 * Implementation of an infiniband connection
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 13.06.2017
 */
class IBConnection extends AbstractConnection<IBPipeIn, IBPipeOut> {
    private static final Logger LOGGER = LogManager.getFormatterLogger(IBConnection.class.getSimpleName());

    private final IBWriteInterestManager m_interestManager;

    /**
     * Constructor
     *
     * @param p_ownNodeId
     *         Node id of the current node
     * @param p_destinationNodeId
     *         Node id of the destination connected to with this connection
     * @param p_sendBufferNativeAddr
     *         Native memory address of the send buffer to use for the ORB
     * @param p_outBufferSize
     *         Size of the outgoing (ring) buffer
     * @param p_flowControlWindowSize
     *         Size of the flow control window
     * @param p_flowControlWindowThreshold
     *         Threshold of the flow control window
     * @param p_messageDirectory
     *         Message directory instance
     * @param p_requestMap
     *         Request map instance
     * @param p_exporterPool
     *         Exporter pool instance
     * @param p_messageHandlers
     *         Message handlers instance
     * @param p_writeInterestManager
     *         Write interest manager instance
     * @param p_benchmarkMode
     *         True to enable benchmark mode and record all RTT values to calculate percentile
     */
    IBConnection(final short p_ownNodeId, final short p_destinationNodeId, final long p_sendBufferNativeAddr,
            final int p_outBufferSize, final int p_flowControlWindowSize, final float p_flowControlWindowThreshold,
            final LocalMessageHeaderPool p_messageHeaderPool, final MessageDirectory p_messageDirectory,
            final RequestMap p_requestMap, final AbstractExporterPool p_exporterPool,
            final MessageHandlers p_messageHandlers, final IBWriteInterestManager p_writeInterestManager,
            final boolean p_benchmarkMode) {
        super(p_ownNodeId);

        IBFlowControl flowControl = new IBFlowControl(p_destinationNodeId, p_flowControlWindowSize,
                p_flowControlWindowThreshold, p_writeInterestManager);
        IBOutgoingRingBuffer outgoingBuffer = new IBOutgoingRingBuffer(p_destinationNodeId, p_sendBufferNativeAddr,
                p_outBufferSize, p_exporterPool);
        IBPipeIn pipeIn = new IBPipeIn(p_ownNodeId, p_destinationNodeId, p_messageHeaderPool, flowControl,
                p_messageDirectory, p_requestMap, p_messageHandlers, p_benchmarkMode);
        IBPipeOut pipeOut = new IBPipeOut(p_ownNodeId, p_destinationNodeId, flowControl, outgoingBuffer,
                p_writeInterestManager);

        setPipes(pipeIn, pipeOut);

        m_interestManager = p_writeInterestManager;
    }

    @Override
    public void close(final boolean p_force) {
        setClosingTimestamp(System.currentTimeMillis());

        if (!p_force) {
            if (!getPipeOut().isOutgoingQueueEmpty()) {
                LOGGER.debug("Waiting for all scheduled messages to be sent over to be closed connection!");

                long start = System.currentTimeMillis();

                while (!getPipeOut().isOutgoingQueueEmpty()) {
                    Thread.yield();

                    if (System.currentTimeMillis() - start > 2000) {
                        LOGGER.debug("Waiting for all scheduled messages to be sent over aborted, timeout");
                        break;
                    }
                }
            }
        }

        // flush any remaining interests
        m_interestManager.nodeDisconnected(getOwnNodeID());
    }

    @Override
    public void wakeup() {
        // nothing to do here
    }
}
