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

package de.hhu.bsinfo.dxnet.nio;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxutils.UnsafeHandler;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import de.hhu.bsinfo.dxutils.stats.Time;
import de.hhu.bsinfo.dxutils.stats.TimePool;

/**
 * Interest queue based on an array and an ArrayList.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 18.12.2017
 */
class InterestQueue {
    private static final Logger LOGGER = LogManager.getFormatterLogger(InterestQueue.class.getSimpleName());

    private static final TimePool SOP_ADD = new TimePool(InterestQueue.class, "Add");
    private static final Time SOP_PROCESS = new Time(InterestQueue.class, "Process");

    static {
        StatisticsManager.get().registerOperation(InterestQueue.class, SOP_ADD);
        StatisticsManager.get().registerOperation(InterestQueue.class, SOP_PROCESS);
    }

    // Operations (0b1, 0b10, 0b100, 0b1000 reserved in SelectionKey)
    static final byte READ = 1;
    static final byte WRITE = 1 << 2;
    static final byte CONNECT = 1 << 3;
    static final byte READ_FLOW_CONTROL = 1 << 5;
    static final byte WRITE_FLOW_CONTROL = 1 << 6;
    static final byte CLOSE = 1 << 1;

    private byte[] m_changeRequests;
    private ArrayList<NIOConnection> m_activeConnections;

    private ReentrantLock m_changeLock;

    // Constructors

    /**
     * Creates an instance of InterestQueue
     */
    InterestQueue() {
        m_changeRequests = new byte[(int) Math.pow(2, 16)];
        m_activeConnections = new ArrayList<>();
        m_changeLock = new ReentrantLock(false);
    }

    /**
     * Adds an operation interest for given connection.
     *
     * @param p_interest
     *         the operation interest.
     * @param p_connection
     *         the connection.
     * @return true, if the interest was not yet registered.
     */
    boolean addInterest(final byte p_interest, final NIOConnection p_connection) {
        boolean ret = false;
        byte oldInterest;
        short nodeID = p_connection.getDestinationNodeID();

        SOP_ADD.startDebug();

        // Shortcut: if interest was already set (e.g. WRITE), we return immediately without locking (happens very
        // often; once per message).
        // Other threads in this section can only add further interests which will not affect this thread's interest.
        // The selector thread might be processing the interests in parallel and might have reset the interest already
        // (not visible yet as the lock has not been released). But, the operation (e.g. writing to the channel) has
        // not been started as the selector thread must leave the critical area first.
        UnsafeHandler.getInstance().getUnsafe().loadFence();

        if (m_changeRequests[nodeID & 0xFFFF] == p_interest) {
            return false;
        }

        m_changeLock.lock();
        oldInterest = m_changeRequests[nodeID & 0xFFFF];

        if (oldInterest == 0) {
            // Connection was not registered since last processing -> add to active connections
            m_activeConnections.add(p_connection);

            if (m_activeConnections.size() == 1) {
                // Wake-up the NIOSelector thread on return
                ret = true;
            }
        }

        m_changeRequests[nodeID & 0xFFFF] = (byte) (oldInterest | p_interest);
        m_changeLock.unlock();

        SOP_ADD.stopDebug();

        return ret;
    }

    /**
     * Processes all registered operation interests.
     *
     * @param p_selector
     *         the Selector.
     * @param p_connectionManager
     *         the connection Manager.
     * @param p_connectionTimeout
     *         the configured connection timeout.
     */
    void processInterests(final Selector p_selector, final NIOConnectionManager p_connectionManager,
            final int p_connectionTimeout) {
        int interest;
        int entries;
        SelectionKey key;
        NIOConnection connection;

        SOP_PROCESS.startDebug();

        m_changeLock.lock();
        entries = m_activeConnections.size();

        if (entries > 0) {
            for (int i = 0; i < m_activeConnections.size(); i++) {
                connection = m_activeConnections.get(i);
                // Get interest of this connection
                interest = m_changeRequests[connection.getDestinationNodeID() & 0xFFFF];
                // Reset interest for this connection
                m_changeRequests[connection.getDestinationNodeID() & 0xFFFF] = 0;

                /*
                 * By aggregating different interests (CONNECT, READ_FLOW_CONTROL, READ, WRITE_FLOW_CONTROL, WRITE
                 * and CLOSE) for one connection, we loose the ordering within those interests. But, this is not a
                 * problem at all as the ordering is implicitly (we favor flow control over data transfer):
                 *     1) CONNECT              - a connection must be connected before using it
                 *     2) READ_FLOW_CONTROL    - after creating the PipeOut, the incoming stream must be registered
                 *                               for reading flow control updates
                 *     3) READ                 - after creating the PipeIn, the incoming stream must be registered
                 *                               for reading data
                 *     4) WRITE_FLOW_CONTROL         - there is a new flow control update to be written to the outgoing
                 *                                     stream of the PipeIn
                 *     5) WRITE                - there is new data to be written to the outgoing stream of the PipeOut
                 *     6) CLOSE                - connection closure is always the last operation as all following will
                 *                               fail (without re-opening the connection)
                 *
                 */
                if ((interest & CONNECT) == CONNECT) {
                    // CONNECT -> register with connection as attachment (ACCEPT is registered directly)
                    try {
                        connection.getPipeOut().getChannel().register(p_selector, SelectionKey.OP_CONNECT, connection);
                    } catch (final ClosedChannelException e) {
                        LOGGER.debug("Could not change operations!");
                    }
                }
                if ((interest & READ_FLOW_CONTROL) == READ_FLOW_CONTROL) {
                    try {
                        // This is a READ access for flow control - CALLED ONCE AFTER CONNECTION CREATION
                        try {
                            // Use outgoing channel for receiving flow control messages
                            connection.getPipeOut().getChannel().register(p_selector, SelectionKey.OP_READ, connection);
                        } catch (ClosedChannelException e) {
                            e.printStackTrace();
                        }
                    } catch (final CancelledKeyException e) {
                        // Ignore
                    }
                }
                if ((interest & READ) == READ) {
                    try {
                        // This is a READ access - CALLED ONCE AFTER CONNECTION CREATION
                        try {
                            // Use incoming channel for receiving messages
                            connection.getPipeIn().getChannel().register(p_selector, SelectionKey.OP_READ, connection);
                        } catch (ClosedChannelException e) {
                            e.printStackTrace();
                        }
                    } catch (final CancelledKeyException e) {
                        // Ignore
                    }
                }

                if ((interest & WRITE_FLOW_CONTROL) == WRITE_FLOW_CONTROL) {
                    try {
                        // This is a WRITE_FLOW_CONTROL access - Write flow control bytes over incoming channel
                        key = connection.getPipeIn().getChannel().keyFor(p_selector);
                        if (key != null && key.interestOps() != (SelectionKey.OP_READ | SelectionKey.OP_WRITE)) {
                            // Key might be null if connection was closed during shutdown or due to closing a duplicate
                            // connection
                            // If key interest is READ | WRITE the interest must not be overwritten with WRITE as both
                            // incoming buffers might be filled causing a deadlock
                            key.interestOps(SelectionKey.OP_WRITE);
                        }
                    } catch (final CancelledKeyException e) {
                        // Ignore
                    }

                }
                if ((interest & WRITE) == WRITE) {
                    try {
                        // This is a WRITE access -> change interest only
                        key = connection.getPipeOut().getChannel().keyFor(p_selector);
                        if (key == null) {
                            // Key might be null if connection was closed during shutdown or due to closing a duplicate
                            // connection
                            LOGGER.error("Cannot register WRITE operation as key is null for %s", connection);
                        } else if (key.interestOps() != (SelectionKey.OP_READ | SelectionKey.OP_WRITE)) {
                            // If key interest is READ | WRITE the interest must not be overwritten with WRITE as both
                            // incoming buffers might be filled causing a deadlock
                            key.interestOps(SelectionKey.OP_WRITE);
                        }
                    } catch (final CancelledKeyException e) {
                        // Ignore
                    }
                }
                if ((interest & CLOSE) == CLOSE) {
                    // This has to be at the end to avoid missing last messages of a node
                    // CLOSE -> close connection
                    // Close connection after at least two connection timeouts since request
                    if (System.currentTimeMillis() - connection.getClosingTimestamp() > 2 * p_connectionTimeout) {
                        SocketAddress socketAddress = null;
                        try {
                            socketAddress = connection.getPipeOut().getChannel().getRemoteAddress();
                        } catch (final IOException ignored) {
                        }
                        if (socketAddress != null) {
                            LOGGER.debug("Closing connection to 0x%X;%s", connection.getDestinationNodeID(),
                                    socketAddress);
                        }
                        // Close connection
                        p_connectionManager.closeConnection(connection, false);
                    } else {
                        // Delay connection closure
                        m_changeRequests[connection.getDestinationNodeID() & 0xFFFF] = CLOSE;
                        m_activeConnections.add(connection);
                    }
                }
            }
            // Remove active connections as we processed all interests
            if (entries == m_activeConnections.size()) {
                m_activeConnections.clear();
            } else {
                // "New" close interests registered -> delete everything in front
                m_activeConnections.subList(0, entries).clear();
            }
        }
        m_changeLock.unlock();

        SOP_PROCESS.stopDebug();
    }
}
