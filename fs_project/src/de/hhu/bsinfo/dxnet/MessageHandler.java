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

package de.hhu.bsinfo.dxnet;

import java.util.concurrent.locks.LockSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxnet.core.LocalMessageHeaderPool;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.MessageHeader;
import de.hhu.bsinfo.dxnet.core.MessageHeaderPool;
import de.hhu.bsinfo.dxnet.core.MessageImporterCollection;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import de.hhu.bsinfo.dxutils.stats.Time;

/**
 * Executes incoming default messages
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 19.07.2016
 */
class MessageHandler extends Thread {
    private static final Logger LOGGER = LogManager.getFormatterLogger(MessageHandler.class.getSimpleName());

    private static final Time SOP_CREATE = new Time(MessageHandler.class, "CreateAndImport");
    private static final Time SOP_EXECUTE = new Time(MessageHandler.class, "Execute");

    static {
        StatisticsManager.get().registerOperation(MessageHandler.class, SOP_CREATE);
        StatisticsManager.get().registerOperation(MessageHandler.class, SOP_EXECUTE);
    }

    // optimized values determined by experiments
    private static final int THRESHOLD_TIME_CHECK = 100000;

    private final MessageReceiverStore m_messageReceivers;
    private final MessageHeaderStore m_messages;
    private final MessageImporterCollection m_importers;
    private final LocalMessageHeaderPool m_messageHeaderPool;

    private byte m_specialReceiveType = -1;
    private byte m_specialReceiveSubtype = -1;

    private volatile boolean m_overprovisioning;
    private volatile boolean m_shutdown;

    // Constructors

    /**
     * Creates an instance of MessageHandler
     *
     * @param p_queue
     *         the message queue
     */
    MessageHandler(final MessageReceiverStore p_messageReceivers, final MessageHeaderStore p_queue,
            final MessageHeaderPool p_messageHeaderPool,
            final boolean p_overprovisioning) {
        m_messageReceivers = p_messageReceivers;
        m_messages = p_queue;
        m_importers = new MessageImporterCollection();
        m_messageHeaderPool = new LocalMessageHeaderPool(p_messageHeaderPool);

        m_overprovisioning = p_overprovisioning;
    }

    /**
     * Closes the handler
     */
    public void shutdown() {
        m_shutdown = true;
    }

    /**
     * Activate parking strategy.
     */
    void activateParking() {
        m_overprovisioning = true;
    }

    /**
     * Registers a special receive message type
     *
     * @param p_type
     *         the unique type
     * @param p_subtype
     *         the unique subtype
     */
    void registerSpecialReceiveMessageType(final byte p_type, final byte p_subtype) {
        if (m_specialReceiveType == -1 && m_specialReceiveSubtype == -1) {
            m_specialReceiveType = p_type;
            m_specialReceiveSubtype = p_subtype;
        } else {
            // #if LOGGER >= ERROR
            LOGGER.error("Special receive type already registered: %d, %d!", p_type, p_subtype);
            // #endif /* LOGGER >= ERROR */
        }
    }

    // Methods
    @Override
    public void run() {
        int counter = 0;
        long lastSuccessfulPop = 0;
        MessageHeader header;
        Message message;
        MessageReceiver messageReceiver;
        byte type;
        byte subtype;

        while (!m_shutdown) {
            header = m_messages.popMessageHeader();

            if (header == null) {
                if (++counter >= THRESHOLD_TIME_CHECK) {
                    if (System.currentTimeMillis() - lastSuccessfulPop >
                            1000) { // No message header for over a second -> sleep
                        LockSupport.parkNanos(100);
                    }
                }

                if (m_overprovisioning) {
                    LockSupport.parkNanos(1);
                }

                continue;
            }
            lastSuccessfulPop = System.currentTimeMillis();
            counter = 0;

            // #ifdef STATISTICS
            SOP_CREATE.start();
            // #endif /* STATISTICS */

            type = header.getType();
            subtype = header.getSubtype();
            if (type == m_specialReceiveType && subtype == m_specialReceiveSubtype) {
                // This is a special case for DXRAM's logging to deserialize the message's chunks directly into the write buffer.
                // Do not use this method without considering all other possibilities!
                if (!header.isIncomplete()) {
                    messageReceiver = m_messageReceivers.getReceiver(type, subtype);
                    if (messageReceiver != null) {
                        // #ifdef STATISTICS
                        SOP_EXECUTE.start();
                        // #endif /* STATISTICS */

                        ((SpecialMessageReceiver) messageReceiver).onIncomingHeader(header);
                        header.finishHeader(m_messageHeaderPool);

                        // #ifdef STATISTICS
                        SOP_EXECUTE.stop();
                        // #endif /* STATISTICS */
                    } else {
                        // #if LOGGER >= ERROR
                        LOGGER.error("No message receiver was registered for %d, %d!", type, subtype);
                        // #endif /* LOGGER >= ERROR */
                    }
                    continue;
                } else {
                    // If header is incomplete, the deserialization was already started and a message object
                    // created by the MessageCreationCoordinator. In this case use the default way by continuing
                    // the deserialization and finishing the message object.
                }
            }

            try {
                message = header.createAndImportMessage(m_importers, m_messageHeaderPool);
            } catch (NetworkException e) {
                e.printStackTrace();
                continue;
            }

            // #ifdef STATISTICS
            SOP_CREATE.stop();
            // #endif /* STATISTICS */

            if (message != null) {
                messageReceiver = m_messageReceivers.getReceiver(type, subtype);
                if (messageReceiver != null) {
                    // #ifdef STATISTICS
                    SOP_EXECUTE.start();
                    // #endif /* STATISTICS */

                    messageReceiver.onIncomingMessage(message);

                    // #ifdef STATISTICS
                    SOP_EXECUTE.stop();
                    // #endif /* STATISTICS */
                } else {
                    // #if LOGGER >= ERROR
                    LOGGER.error("No message receiver was registered for %d, %d!", type, subtype);
                    // #endif /* LOGGER >= ERROR */
                }
            }
        }
    }
}
