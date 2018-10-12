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

import java.util.concurrent.locks.LockSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import de.hhu.bsinfo.dxutils.stats.Value;

/**
 * The MessageCreationCoordinator builds messages and forwards them to the MessageHandlers.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 31.05.2016
 */
public class MessageCreationCoordinator extends Thread {
    private static final Logger LOGGER = LogManager.getFormatterLogger(
            MessageCreationCoordinator.class.getSimpleName());

    private static final Value SOP_PARKING = new Value(MessageCreationCoordinator.class, "Parking");
    private static final Value SOP_YIELDING = new Value(MessageCreationCoordinator.class, "Yielding");

    static {
        StatisticsManager.get().registerOperation(MessageCreationCoordinator.class, SOP_PARKING);
        StatisticsManager.get().registerOperation(MessageCreationCoordinator.class, SOP_YIELDING);
    }

    private static final int THRESHOLD_TIME_CHECK = 100000;

    private IncomingBufferQueue m_bufferQueue;

    private volatile boolean m_overprovisioning;
    private volatile boolean m_shutdown;

    /**
     * Creates an instance of MessageCreationCoordinator
     *
     * @param p_incomingQueueMaxCapacityBufferCount
     *         the max capacity of buffers (count) for the incoming queue
     * @param p_incomingQueueMaxCapacitySize
     *         the max capacity of all buffers aggregated sizes for the incoming queue
     * @param p_overprovisioning
     *         True to enable overprivisioning
     */
    public MessageCreationCoordinator(final int p_incomingQueueMaxCapacityBufferCount,
            final int p_incomingQueueMaxCapacitySize,
            final boolean p_overprovisioning) {
        m_bufferQueue = new IncomingBufferQueue(p_incomingQueueMaxCapacityBufferCount, p_incomingQueueMaxCapacitySize);
        m_overprovisioning = p_overprovisioning;
    }

    /**
     * Returns the incoming buffer queue
     *
     * @return the IncomingBufferQueue
     */
    public IncomingBufferQueue getIncomingBufferQueue() {
        return m_bufferQueue;
    }

    /**
     * Activate overprovisioning.
     */
    public void activateParking() {
        m_overprovisioning = true;
    }

    @Override
    public void run() {
        IncomingBufferQueue.IncomingBuffer incomingBuffer;
        int counter = 0;
        long lastSuccessfulPop = 0;
        boolean pollWait = true;

        while (!m_shutdown) {
            // pop an incomingBuffer
            incomingBuffer = m_bufferQueue.popBuffer();

            if (incomingBuffer == null) {
                // Ring-buffer is empty.
                if (m_overprovisioning) {
                    SOP_YIELDING.inc();

                    Thread.yield();
                } else {
                    if (pollWait) {
                        if (++counter >= THRESHOLD_TIME_CHECK) {
                            if (System.currentTimeMillis() - lastSuccessfulPop >
                                    100) { // No message header for over a second -> sleep
                                pollWait = false;
                            }
                        }
                    }

                    if (!pollWait) {
                        SOP_PARKING.inc();

                        LockSupport.parkNanos(1000);
                    }
                }

                continue;
            }

            pollWait = true;

            lastSuccessfulPop = System.currentTimeMillis();
            counter = 0;

            try {
                incomingBuffer.getPipeIn().processBuffer(incomingBuffer);
            } catch (final NetworkException e) {
                incomingBuffer.getPipeIn().returnProcessedBuffer(incomingBuffer.getDirectBuffer(),
                        incomingBuffer.getBufferHandle());

                LOGGER.error("Processing incoming buffer failed", e);
            }
        }
    }

    /**
     * Shutdown the message creator thread
     */
    public void shutdown() {
        LOGGER.info("Message creator shutdown...");

        m_shutdown = true;

        try {
            // wait a moment for the thread to shut down (if it can)
            Thread.sleep(100);
        } catch (final InterruptedException ignore) {

        }

        interrupt();
        LockSupport.unpark(this);
        try {
            join();
        } catch (final InterruptedException ignore) {
        }
    }

}
