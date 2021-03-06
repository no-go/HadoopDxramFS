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

import java.net.InetSocketAddress;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxnet.MessageHandlers;
import de.hhu.bsinfo.dxnet.NetworkDestinationUnreachableException;
import de.hhu.bsinfo.dxnet.NodeMap;
import de.hhu.bsinfo.dxnet.core.AbstractConnection;
import de.hhu.bsinfo.dxnet.core.AbstractConnectionManager;
import de.hhu.bsinfo.dxnet.core.AbstractExporterPool;
import de.hhu.bsinfo.dxnet.core.CoreConfig;
import de.hhu.bsinfo.dxnet.core.DynamicExporterPool;
import de.hhu.bsinfo.dxnet.core.IncomingBufferQueue;
import de.hhu.bsinfo.dxnet.core.LocalMessageHeaderPool;
import de.hhu.bsinfo.dxnet.core.MessageDirectory;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxnet.core.NetworkRuntimeException;
import de.hhu.bsinfo.dxnet.core.RequestMap;
import de.hhu.bsinfo.dxnet.core.StaticExporterPool;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.UnsafeHandler;
import de.hhu.bsinfo.dxutils.stats.AbstractOperation;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import de.hhu.bsinfo.dxutils.stats.TimePool;
import de.hhu.bsinfo.dxutils.stats.Timeline;
import de.hhu.bsinfo.dxutils.stats.Value;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;

/**
 * Connection manager for infiniband (note: this is the main class for the IB subsystem in the java space)
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 13.06.2017
 */
@SuppressWarnings("sunapi")
public class IBConnectionManager extends AbstractConnectionManager
        implements MsgrcJNIBinding.CallbackHandler, NodeMap.Listener {
    private static final Logger LOGGER = LogManager.getFormatterLogger(IBConnectionManager.class.getSimpleName());

    private static final TimePool SOP_CREATE_CON = new TimePool(IBConnectionManager.class, "CreateCon");
    private static final Timeline SOP_RECV = new Timeline(IBConnectionManager.class, "Recv", "Process", "Native");
    private static final Timeline SOP_SEND_NEXT_DATA = new Timeline(IBConnectionManager.class, "SendNextData",
            "PrevResults", "SendComps", "NextData", "Native");
    private static final Value SOP_NEXT_DATA_NONE = new Value(IBConnectionManager.class, "NextDataNone");
    private static final Value SOP_SEND_DATA_POSTED = new Value(IBConnectionManager.class, "SendDataPosted");
    private static final Value SOP_SEND_DATA_POSTED_NONE = new Value(IBConnectionManager.class, "SendDataPostedNone");
    private static final Value SOP_SEND_DATA_AVAIL = new Value(IBConnectionManager.class, "SendDataAvail");
    private static final SendRecvTargetStats SOP_SEND_TARGET = new SendRecvTargetStats(IBConnectionManager.class,
            "SendTarget");
    private static final SendRecvTargetStats SOP_RECV_TARGET = new SendRecvTargetStats(IBConnectionManager.class,
            "RecvTarget");

    static {
        StatisticsManager.get().registerOperation(IBConnectionManager.class, SOP_CREATE_CON);
        StatisticsManager.get().registerOperation(IBConnectionManager.class, SOP_RECV);
        StatisticsManager.get().registerOperation(IBConnectionManager.class, SOP_SEND_NEXT_DATA);
        StatisticsManager.get().registerOperation(IBConnectionManager.class, SOP_NEXT_DATA_NONE);
        StatisticsManager.get().registerOperation(IBConnectionManager.class, SOP_SEND_DATA_POSTED);
        StatisticsManager.get().registerOperation(IBConnectionManager.class, SOP_SEND_DATA_POSTED_NONE);
        StatisticsManager.get().registerOperation(IBConnectionManager.class, SOP_SEND_DATA_AVAIL);
        StatisticsManager.get().registerOperation(IBConnectionManager.class, SOP_SEND_TARGET);
        StatisticsManager.get().registerOperation(IBConnectionManager.class, SOP_RECV_TARGET);
    }

    private final CoreConfig m_coreConfig;
    private final IBConfig m_config;
    private final NodeMap m_nodeMap;

    private final MessageDirectory m_messageDirectory;
    private final RequestMap m_requestMap;
    private final IncomingBufferQueue m_incomingBufferQueue;
    private final LocalMessageHeaderPool m_messageHeaderPool;
    private final MessageHandlers m_messageHandlers;

    private AbstractExporterPool m_exporterPool;

    private final IBWriteInterestManager m_writeInterestManager;

    private final boolean[] m_nodeDiscovered;

    private NextWorkPackage m_nextWorkPackage;
    private PrevWorkPackageResults m_prevWorkPackageResults;
    private CompletedWorkList m_completedWorkList;
    private IncomingRingBuffer m_incomingRingBuffer;

    /**
     * Constructor
     *
     * @param p_coreConfig
     *         Core configuration instance with core config values
     * @param p_config
     *         IB configuration instance with IB specific config values
     * @param p_nodeMap
     *         Node map instance
     * @param p_messageDirectory
     *         Message directory instance
     * @param p_requestMap
     *         Request map instance
     * @param p_incomingBufferQueue
     *         Incoming buffer queue instance
     * @param p_messageHeaderPool
     *         Pool for message headers
     * @param p_messageHandlers
     *         Message handlers instance
     * @param p_overprovisioning
     *         True if overprovisioning is on, false for off
     */
    public IBConnectionManager(final CoreConfig p_coreConfig, final IBConfig p_config, final NodeMap p_nodeMap,
            final MessageDirectory p_messageDirectory,
            final RequestMap p_requestMap, final IncomingBufferQueue p_incomingBufferQueue,
            final LocalMessageHeaderPool p_messageHeaderPool,
            final MessageHandlers p_messageHandlers, final boolean p_overprovisioning) {
        super(p_config.getMaxConnections(), p_overprovisioning);

        m_coreConfig = p_coreConfig;
        m_config = p_config;
        m_nodeMap = p_nodeMap;

        m_messageDirectory = p_messageDirectory;
        m_requestMap = p_requestMap;
        m_incomingBufferQueue = p_incomingBufferQueue;
        m_messageHeaderPool = p_messageHeaderPool;
        m_messageHandlers = p_messageHandlers;

        if (p_coreConfig.isUseStaticExporterPool()) {
            m_exporterPool = new StaticExporterPool();
        } else {
            m_exporterPool = new DynamicExporterPool();
        }

        m_writeInterestManager = new IBWriteInterestManager();

        m_nodeDiscovered = new boolean[NodeID.MAX_ID];
    }

    /**
     * Initialize the infiniband subsystem. This calls to the underlying Ibdxnet subsystem and requires the respective
     * library to be loaded
     */
    public void init() {
        try {
            // can't call this in the constructor because it relies on the implemented interfaces for callbacks
            if (!MsgrcJNIBinding.init(this, m_config.isPinSendRecvThreads(), m_config.isEnableSignalHandler(),
                    m_config.getStatisticsThreadPrintIntervalMs(), m_coreConfig.getOwnNodeId(),
                    (int) m_config.getConnectionCreationTimeout().getMs(), m_config.getMaxConnections(),
                    m_config.getSqSize(), m_config.getSrqSize(), m_config.getSharedSCQSize(),
                    m_config.getSharedRCQSize(),
                    (int) m_config.getOutgoingRingBufferSize().getBytes(),
                    m_config.getIncomingBufferPoolTotalSize().getBytes(),
                    (int) m_config.getIncomingBufferSize().getBytes(), m_config.getMaxSGEs())) {

                LOGGER.debug("Initializing ib transport failed, check ibnet logs");

                throw new NetworkRuntimeException("Initializing ib transport failed");
            }
        } catch (UnsatisfiedLinkError ignored) {
            throw new NetworkRuntimeException("Initializing ib transport failed, could not find init method. It's " +
                    "likely that the native library couldn't be loaded because it's not available.");
        }

        // register listener first
        m_nodeMap.registerListener(this);

        // wait a moment to ensure the list is up to date
        try {
            Thread.sleep(500);
        } catch (InterruptedException ignored) {
        }

        // now get all nodes that were reported before we had the listener registered
        List<NodeMap.Mapping> nodes = m_nodeMap.getAvailableMappings();

        for (NodeMap.Mapping node : nodes) {
            byte[] bytes = node.getAddress().getAddress().getAddress();
            int val = (int) (((long) bytes[0] & 0xFF) << 24 | ((long) bytes[1] & 0xFF) << 16 |
                    ((long) bytes[2] & 0xFF) << 8 | bytes[3] & 0xFF);
            MsgrcJNIBinding.addNode(val);
        }

        // wait a little to allow brief initial node discovery
        try {
            Thread.sleep(2000);
        } catch (InterruptedException ignored) {
        }
    }

    @Override
    public void close() {
        LOGGER.debug("Closing connection manager");

        super.close();

        MsgrcJNIBinding.shutdown();
    }

    @Override
    protected AbstractConnection createConnection(final short p_destination,
            final AbstractConnection p_existingConnection) throws NetworkException {
        IBConnection connection;

        if (!m_nodeDiscovered[p_destination & 0xFFFF]) {
            throw new NetworkDestinationUnreachableException(p_destination);
        }

        SOP_CREATE_CON.start();

        if (m_openConnections == m_maxConnections) {
            LOGGER.debug("Connection max (%d) reached, dismissing random connection", m_maxConnections);

            dismissRandomConnection();
        }

        // force connection creation in native subsystem
        int res = MsgrcJNIBinding.createConnection(p_destination);

        if (res != 0) {
            if (res == 1) {
                LOGGER.debug("Connection creation (0x%X) time-out. Interval %s ms might be to small", p_destination,
                        m_config.getConnectionCreationTimeout());

                SOP_CREATE_CON.stop();

                throw new NetworkException("Connection creation timeout occurred");
            } else {
                LOGGER.error("Connection creation (0x%X) failed", p_destination);

                SOP_CREATE_CON.stop();

                throw new NetworkException("Connection creation failed");
            }
        }

        long sendBufferAddr = MsgrcJNIBinding.getSendBufferAddress(p_destination);

        if (sendBufferAddr == -1) {
            SOP_CREATE_CON.stop();

            // might happen on disconnect or if connection is not established in the ibnet subsystem
            throw new NetworkDestinationUnreachableException(p_destination);
        }

        LOGGER.debug("Node connected 0x%X, ORB native addr 0x%X", p_destination, sendBufferAddr);

        connection = new IBConnection(m_coreConfig.getOwnNodeId(), p_destination, sendBufferAddr,
                (int) m_config.getOutgoingRingBufferSize().getBytes(), (int) m_config.getFlowControlWindow().getBytes(),
                m_config.getFlowControlWindowThreshold(), m_messageHeaderPool, m_messageDirectory, m_requestMap,
                m_exporterPool, m_messageHandlers, m_writeInterestManager, m_coreConfig.isBenchmarkMode());

        connection.setPipeInConnected(true);
        connection.setPipeOutConnected(true);

        m_openConnections++;

        SOP_CREATE_CON.stop();

        return connection;
    }

    @Override
    protected void closeConnection(final AbstractConnection p_connection, final boolean p_removeConnection) {
        LOGGER.debug("Closing connection 0x%X", p_connection.getDestinationNodeID());

        p_connection.setPipeInConnected(false);
        p_connection.setPipeOutConnected(false);

        AbstractConnection tmp = m_connections[p_connection.getDestinationNodeID() & 0xFFFF];

        if (p_connection.equals(tmp)) {
            p_connection.close(p_removeConnection);
            m_connections[p_connection.getDestinationNodeID() & 0xFFFF] = null;
            m_openConnections--;
        }

        // Trigger failure handling for remote node over faulty connection
        if (m_listener != null) {
            m_listener.connectionLost(p_connection.getDestinationNodeID());
        }
    }

    @Override
    public void nodeDiscovered(final short p_nodeId) {
        LOGGER.debug("Node discovered 0x%X", p_nodeId);

        m_nodeDiscovered[p_nodeId & 0xFFFF] = true;
    }

    @Override
    public void nodeInvalidated(final short p_nodeId) {
        LOGGER.debug("Node invalidated 0x%X", p_nodeId);

        m_nodeDiscovered[p_nodeId & 0xFFFF] = false;
    }

    @Override
    public void nodeDisconnected(final short p_nodeId) {
        LOGGER.debug("Node disconnected 0x%X", p_nodeId);

        closeConnection(m_connections[p_nodeId & 0xFFFF], true);
    }

    @Override
    public int received(final long p_incomingRingBuffer) {
        int processed = 0;

        try {
            SOP_RECV.stopDebug();
            SOP_RECV.startDebug();

            // wrap on the first callback, native address is always the same
            if (m_incomingRingBuffer == null) {
                m_incomingRingBuffer = new IncomingRingBuffer(p_incomingRingBuffer);

                // for debugging purpose
                Thread.currentThread().setName("IBRecv-native");
            }

            int usedEntries = m_incomingRingBuffer.getUsedEntries();
            int front = m_incomingRingBuffer.getFront();
            int size = m_incomingRingBuffer.getSize();

            for (int i = 0; i < usedEntries; i++) {
                int offset = (front + i) % size;

                short sourceNodeId = m_incomingRingBuffer.getSourceNodeId(offset);
                int fcData = m_incomingRingBuffer.getFcData(offset);
                int dataLength = m_incomingRingBuffer.getDataLength(offset);
                long ptrDataHandle = m_incomingRingBuffer.getData(offset);
                long ptrData = m_incomingRingBuffer.getDataRaw(offset);

                // packages might have no data and no fc data because it was a fc data only package and the IBQ
                // was full in one of the previous iterations. Thus, we processed the fc data with a higher priority
                // in a separate loop further below and cleared it to avoid duplicate processing

                IBConnection connection;

                try {
                    connection = (IBConnection) getConnection(sourceNodeId);
                } catch (final NetworkException e) {
                    LOGGER.error("Getting connection for recv of node 0x%X failed", sourceNodeId, e);

                    processed++;

                    // if that happens we lose data which is quite bad...I don't see any proper fix for this atm
                    continue;
                }

                // FIXME excludeInvocations in types.gradle does NOT remove this for some reason
                // SOP_RECV_TARGET.sendRecvCallDebug(sourceNodeId);

                if (dataLength > 0) {
                    if (!m_incomingBufferQueue.pushBuffer(connection, null, ptrDataHandle, ptrData, dataLength)) {
                        break;
                    }

                    // FIXME excludeInvocations in types.gradle does NOT remove this for some reason
                    // SOP_RECV_TARGET.dataSentRecvDebug(sourceNodeId, dataLength);
                }

                if (fcData > 0) {
                    // process FC AFTER data to avoid processing FC data multiple times (see further below IBQ full)
                    connection.getPipeIn().handleFlowControlData(fcData);
                    // FIXME excludeInvocations in types.gradle does NOT remove this for some reason
                    // SOP_RECV_TARGET.dataFCSentRecvDebug(sourceNodeId, fcData);
                }

                processed++;
            }

            if (processed != usedEntries) {
                // IBQ is full but there might be some more flow control to process
                // don't wait for IBQ to empty but process now instead
                // to avoid processing any FC data which is attached to so far unprocessed buffers again,
                // we have to clear it explicitly
                for (int i = processed; i < usedEntries; i++) {
                    int offset = (front + i) % size;

                    short sourceNodeId = m_incomingRingBuffer.getSourceNodeId(offset);
                    int fcData = m_incomingRingBuffer.getFcData(offset);

                    if (fcData > 0) {
                        IBConnection connection;

                        try {
                            connection = (IBConnection) getConnection(sourceNodeId);
                        } catch (final NetworkException e) {
                            LOGGER.error("Getting connection for recv of node 0x%X failed", sourceNodeId, e);

                            processed++;

                            // if that happens we lose data which is quite bad...I don't see any proper fix for this atm
                            continue;
                        }

                        connection.getPipeIn().handleFlowControlData(fcData);
                        // FIXME excludeInvocations in types.gradle does NOT remove this for some reason
                        // SOP_RECV_TARGET.dataFCSentRecvDebug(sourceNodeId, fcData);

                        // clear data to avoid duplicate processing
                        m_incomingRingBuffer.clearFcData(offset);
                    }
                }
            }

            SOP_RECV.nextSectionDebug();
        } catch (Exception e) {
            // print error because we disabled exception handling when executing jni calls

            LOGGER.error("received unhandled exception", e);
        }

        return processed;
    }

    @Override
    public void getNextDataToSend(final long p_nextWorkPackage, final long p_prevResults, final long p_completionList) {
        try {
            SOP_SEND_NEXT_DATA.stopDebug();
            SOP_SEND_NEXT_DATA.startDebug();

            processPrevResults(p_prevResults);

            SOP_SEND_NEXT_DATA.nextSectionDebug();

            processSendCompletions(p_completionList);

            SOP_SEND_NEXT_DATA.nextSectionDebug();

            prepareNextDataToSend(p_nextWorkPackage);

            SOP_SEND_NEXT_DATA.nextSectionDebug();
        } catch (Exception e) {
            // print error because we disabled exception handling when executing jni calls
            LOGGER.error("getNextDataToSend unhandled exception", e);
        }
    }

    @Override
    public void nodeMappingAdded(final short p_nodeId, final InetSocketAddress p_address) {
        byte[] bytes = p_address.getAddress().getAddress();
        int val = (int) (((long) bytes[0] & 0xFF) << 24 | ((long) bytes[1] & 0xFF) << 16 |
                ((long) bytes[2] & 0xFF) << 8 | bytes[3] & 0xFF);
        MsgrcJNIBinding.addNode(val);
    }

    @Override
    public void nodeMappingRemoved(final short p_nodeId) {

    }

    /**
     * Evaluate the processing results of the previous work package
     *
     * @param p_prevResults
     *         Native pointer to a struct with the data about the previous work package
     */
    private void processPrevResults(final long p_prevResults) {
        // wrap on the first callback, native address is always the same
        if (m_prevWorkPackageResults == null) {
            m_prevWorkPackageResults = new PrevWorkPackageResults(p_prevResults);

            // for debugging purpose
            Thread.currentThread().setName("IBSend-native");
        }

        short nodeId = m_prevWorkPackageResults.getNodeId();

        if (nodeId != NodeID.INVALID_ID) {
            int numBytesPosted = m_prevWorkPackageResults.getNumBytesPosted();
            int numBytesNotPosted = m_prevWorkPackageResults.getNumBytesNotPosted();
            int fcDataPosted = m_prevWorkPackageResults.getFcDataPosted();
            int fcDataNotPosted = m_prevWorkPackageResults.getFcDataNotPosted();

            try {
                IBConnection prevConnection = (IBConnection) getConnection(nodeId);

                prevConnection.getPipeOut().dataSendPosted(numBytesPosted);
                prevConnection.getPipeOut().flowControlDataSendPosted(fcDataPosted);
            } catch (final NetworkException e) {
                LOGGER.error("Getting connection 0x%X for processing prev results failed", nodeId);
            }

            // SendThread could not process all bytes because the queue
            // was full. don't lose the data interest because there is
            // still data to send

            if (numBytesNotPosted > 0) {
                SOP_SEND_DATA_POSTED.add(numBytesPosted);

                m_writeInterestManager.pushBackDataInterest(nodeId);
            } else {
                SOP_SEND_DATA_POSTED_NONE.inc();
            }

            if (fcDataNotPosted > 0) {
                m_writeInterestManager.pushBackFcInterest(nodeId);
            }
        }
    }

    /**
     * Process send completion data which tells us that the asynchronous transfer has completed
     *
     * @param p_completionList
     *         Native pointer to struct with completion data
     */
    private void processSendCompletions(final long p_completionList) {
        // wrap on first call
        if (m_completedWorkList == null) {
            m_completedWorkList = new CompletedWorkList(p_completionList, m_config.getMaxConnections());
        }

        // process callback parameters
        int numItems = m_completedWorkList.getNumNodes();

        if (numItems > 0) {
            // also notify that previous data has been processed (if connection is still available)
            for (int i = 0; i < numItems; i++) {
                short nodeId = m_completedWorkList.getNodeId(i);
                int processedBytes = m_completedWorkList.getNumBytesWritten(nodeId & 0xFFFF);
                int processedFC = m_completedWorkList.getFcDataWritten(nodeId & 0xFFFF);

                // FIXME excludeInvocations in types.gradle does NOT remove this for some reason
                // SOP_SEND_TARGET.dataSentRecvDebug(nodeId, processedBytes);
                // SOP_SEND_TARGET.dataFCSentRecvDebug(nodeId, processedFC);

                try {
                    IBConnection prevConnection = (IBConnection) getConnection(nodeId);

                    prevConnection.getPipeOut().dataSendConfirmed(processedBytes);
                } catch (final NetworkException e) {
                    LOGGER.error("Getting connection 0x%X for processing work completions failed", nodeId);
                }
            }
        }
    }

    /**
     * Prepare the next work package with data from the ORB to send
     *
     * @param p_nextWorkPackage
     *         Native pointer to struct to write the data to for sending
     */
    private void prepareNextDataToSend(final long p_nextWorkPackage) {
        if (m_nextWorkPackage == null) {
            m_nextWorkPackage = new NextWorkPackage(p_nextWorkPackage);
        }

        m_nextWorkPackage.reset();

        // poll for next interest
        short nodeId = m_writeInterestManager.getNextInterests();

        // no data available
        if (nodeId == NodeID.INVALID_ID) {
            return;
        }

        IBConnection connection;

        try {
            connection = (IBConnection) getConnection(nodeId);
        } catch (final NetworkException ignored) {
            m_writeInterestManager.nodeDisconnected(nodeId);
            return;
        }

        long interests = m_writeInterestManager.consumeInterests(nodeId);

        // interest queue and interest count MUST stay on sync. otherwise, something's not right with the
        // interest manager (bug)
        if (interests == 0) {
            throw new IllegalStateException("No interests available but interest manager has write interest set");
        }

        // FIXME excludeInvocations in types.gradle does NOT remove this for some reason
        // SOP_SEND_TARGET.sendRecvCallDebug(nodeId);

        // sets the current work request valid
        m_nextWorkPackage.setNodeId(nodeId);

        int dataInterests = (int) interests;
        int fcInterests = (int) (interests >> 32L);
        boolean nothingToSend = true;

        // process data interests
        if (dataInterests > 0) {
            long pos = connection.getPipeOut().getNextBuffer();
            int relPosFrontRel = (int) (pos >> 32 & 0x7FFFFFFF);
            int relPosBackRel = (int) (pos & 0x7FFFFFFF);

            if (relPosFrontRel != relPosBackRel) {
                // relative position of data start in buffer
                m_nextWorkPackage.setPosBackRel(relPosBackRel);
                // relative position of data end in buffer
                m_nextWorkPackage.setPosFrontRel(relPosFrontRel);

                int dataAvail;

                if (relPosBackRel <= relPosFrontRel) {
                    dataAvail = relPosFrontRel - relPosBackRel;
                } else {
                    dataAvail =
                            (int) (m_config.getOutgoingRingBufferSize().getBytes() - relPosBackRel + relPosFrontRel);
                }

                SOP_SEND_DATA_AVAIL.add(dataAvail);

                nothingToSend = false;
            } else {
                // we got an interest but no data is available because the data was already sent with the previous
                // interest (non harmful data race between ORB and interest manager)
            }
        }

        // process flow control interests
        if (fcInterests > 0) {
            int fcData = connection.getPipeOut().getFlowControlData();

            if (fcData > 0) {
                m_nextWorkPackage.setFlowControlData(fcData);

                nothingToSend = false;
            } else {
                // and again, we got an interest but no FC data is available because the FC data was already sent with
                // the previous interest (non harmful data race between ORB/flow control and interest manager)
            }
        }

        if (nothingToSend) {
            SOP_NEXT_DATA_NONE.inc();

            m_nextWorkPackage.reset();
        }
    }

    /**
     * Wrapper for native struct NextWorkPackage
     */
    private static class NextWorkPackage {
        private static final int SIZE_FIELD_POS_BACK_REL = Integer.BYTES;
        private static final int SIZE_FIELD_POS_FRONT_REL = Integer.BYTES;
        private static final int SIZE_FIELD_FLOW_CONTROL_DATA = Byte.BYTES;
        private static final int SIZE_FIELD_NODE_ID = Short.BYTES;

        private static final int SIZE =
                SIZE_FIELD_POS_BACK_REL + SIZE_FIELD_POS_FRONT_REL + SIZE_FIELD_FLOW_CONTROL_DATA + SIZE_FIELD_NODE_ID;

        private static final int IDX_POS_BACK_REL = 0;
        private static final int IDX_POS_FRONT_REL = IDX_POS_BACK_REL + SIZE_FIELD_POS_BACK_REL;
        private static final int IDX_FLOW_CONTROL_DATA = IDX_POS_FRONT_REL + SIZE_FIELD_POS_FRONT_REL;
        private static final int IDX_NODE_ID = IDX_FLOW_CONTROL_DATA + SIZE_FIELD_FLOW_CONTROL_DATA;

        //    struct NextWorkPackage
        //    {
        //        uint32_t m_posBackRel;
        //        uint32_t m_posFrontRel;
        //        uint8_t m_flowControlData;
        //        con::NodeId m_nodeId;
        //    } __attribute__((packed));
        private final sun.misc.Unsafe m_unsafe;
        private final long m_baseAddress;

        /**
         * Constructor
         *
         * @param p_addr
         *         Native address pointing to struct
         */
        NextWorkPackage(final long p_addr) {
            m_unsafe = UnsafeHandler.getInstance().getUnsafe();
            m_baseAddress = p_addr;
        }

        /**
         * Reset state and clear data
         */
        public void reset() {
            setPosBackRel(0);
            setPosFrontRel(0);
            setFlowControlData((byte) 0);
            setNodeId(NodeID.INVALID_ID);
        }

        /**
         * Set the pos back relative field
         *
         * @param p_pos
         *         Value to set
         */
        void setPosBackRel(final int p_pos) {
            if (p_pos < 0) {
                throw new IllegalStateException("NextWorkPackage posBackRel < 0: " + p_pos);
            }

            m_unsafe.putInt(m_baseAddress + IDX_POS_BACK_REL, p_pos);
        }

        /**
         * Set the pos front relative field
         *
         * @param p_pos
         *         Value to set
         */
        void setPosFrontRel(final int p_pos) {
            if (p_pos < 0) {
                throw new IllegalStateException("NextWorkPackage posFrontRel < 0: " + p_pos);
            }

            m_unsafe.putInt(m_baseAddress + IDX_POS_FRONT_REL, p_pos);
        }

        /**
         * Set data for the flow control data field
         *
         * @param p_data
         *         Value to set
         */
        void setFlowControlData(final int p_data) {
            if (p_data < 0) {
                throw new IllegalStateException("NextWorkPackage fcData < 0: " + p_data);
            }

            if (p_data > 255) {
                throw new IllegalStateException("NextWorkPackage fcData > 255: " + p_data);
            }

            m_unsafe.putByte(m_baseAddress + IDX_FLOW_CONTROL_DATA, (byte) (p_data & 0xFF));
        }

        /**
         * Set the target node id
         *
         * @param p_nodeId
         *         Target node id to send to
         */
        void setNodeId(final short p_nodeId) {
            m_unsafe.putShort(m_baseAddress + IDX_NODE_ID, p_nodeId);
        }
    }

    /**
     * Wrapper for native struct PrevWorkPackageResults
     */
    private static class PrevWorkPackageResults {
        private static final int SIZE_FIELD_NODE_ID = Short.BYTES;
        private static final int SIZE_FIELD_NUM_BYTES_POSTED = Integer.BYTES;
        private static final int SIZE_FIELD_NUM_BYTES_NOT_POSTED = Integer.BYTES;
        private static final int SIZE_FIELD_FC_DATA_POSTED = Byte.BYTES;
        private static final int SIZE_FIELD_FC_DATA_NOT_POSTED = Byte.BYTES;

        private static final int SIZE =
                SIZE_FIELD_NODE_ID + SIZE_FIELD_NUM_BYTES_POSTED + SIZE_FIELD_NUM_BYTES_NOT_POSTED +
                        SIZE_FIELD_FC_DATA_POSTED + SIZE_FIELD_FC_DATA_NOT_POSTED;

        private static final int IDX_NODE_ID = 0;
        private static final int IDX_NUM_BYTES_POSTED = IDX_NODE_ID + SIZE_FIELD_NODE_ID;
        private static final int IDX_NUM_BYTES_NOT_POSTED = IDX_NUM_BYTES_POSTED + SIZE_FIELD_NUM_BYTES_POSTED;
        private static final int IDX_FC_DATA_POSTED = IDX_NUM_BYTES_NOT_POSTED + SIZE_FIELD_NUM_BYTES_NOT_POSTED;
        private static final int IDX_FC_DATA_NOT_POSTED = IDX_FC_DATA_POSTED + SIZE_FIELD_FC_DATA_POSTED;

        //    struct PrevWorkPackageResults
        //    {
        //        con::NodeId m_nodeId;
        //        uint32_t m_numBytesPosted;
        //        uint32_t m_numBytesNotPosted;
        //        uint8_t m_fcDataPosted;
        //        uint8_t m_fcDataNotPosted;
        //    } __attribute__((packed));
        private final sun.misc.Unsafe m_unsafe;
        private final long m_baseAddress;

        /**
         * Constructor
         *
         * @param p_addr
         *         Native address pointing to struct
         */
        PrevWorkPackageResults(final long p_addr) {
            m_unsafe = UnsafeHandler.getInstance().getUnsafe();
            m_baseAddress = p_addr;
        }

        /**
         * Get the node id the last package was sent to
         */
        short getNodeId() {
            return m_unsafe.getShort(m_baseAddress + IDX_NODE_ID);
        }

        /**
         * Get the actual number of bytes posted (might be less than specified on the last work package)
         */
        int getNumBytesPosted() {
            int tmp = m_unsafe.getInt(m_baseAddress + IDX_NUM_BYTES_POSTED);

            if (tmp < 0) {
                throw new IllegalStateException("PrevWorkPackageResults numBytesPosted < 0: " + tmp);
            }

            return tmp;
        }

        /**
         * Get the number of bytes that were not posted on the last work request (due to queue full)
         */
        int getNumBytesNotPosted() {
            int tmp = m_unsafe.getInt(m_baseAddress + IDX_NUM_BYTES_NOT_POSTED);

            if (tmp < 0) {
                throw new IllegalStateException("PrevWorkPackageResults numBytesNotPosted < 0: " + tmp);
            }

            return tmp;
        }

        /**
         * Get the amount of fc data posted on the last work request
         */
        int getFcDataPosted() {
            // use the full range of a uint8_t
            return m_unsafe.getByte(m_baseAddress + IDX_FC_DATA_POSTED) & 0xFF;
        }

        /**
         * Get the amount of fc data could not be posted (queue full)
         */
        int getFcDataNotPosted() {
            // use the full range of a uint8_t
            return m_unsafe.getByte(m_baseAddress + IDX_FC_DATA_NOT_POSTED) & 0xFF;
        }
    }

    /**
     * Wrapper for native struct CompletedWorkList
     */
    private static class CompletedWorkList {
        private static final int SIZE_FIELD_NUM_NODES = Short.BYTES;
        private static final int SIZE_FIELD_NUM_BYTES_WRITTEN = Integer.BYTES;
        private static final int SIZE_FIELD_NUM_BYTES_WRITTEN_ARRAY = SIZE_FIELD_NUM_BYTES_WRITTEN * 0xFFFF;
        private static final int SIZE_FIELD_FC_DATA_WRITTEN = Byte.BYTES;
        private static final int SIZE_FIELD_FC_DATA_WRITTEN_ARRAY = SIZE_FIELD_FC_DATA_WRITTEN * 0xFFFF;
        private static final int SIZE_FIELD_NODE_ID = Short.BYTES;

        private static final int IDX_NUM_ITEMS = 0;
        private static final int IDX_BYTES_WRITTEN = IDX_NUM_ITEMS + SIZE_FIELD_NUM_NODES;
        private static final int IDX_FC_DATA_WRITTEN = IDX_BYTES_WRITTEN + SIZE_FIELD_NUM_BYTES_WRITTEN_ARRAY;
        private static final int IDX_NODE_IDS = IDX_FC_DATA_WRITTEN + SIZE_FIELD_FC_DATA_WRITTEN_ARRAY;

        private final int m_numNodes;

        //    struct CompletedWorkList
        //    {
        //        uint16_t m_numNodes;
        //        uint32_t m_numBytesWritten[con::NODE_ID_MAX_NUM_NODES];
        //        uint8_t m_fcDataWritten[con::NODE_ID_MAX_NUM_NODES];
        //        con::NodeId m_nodeIds[];
        //    } __attribute__((packed));
        private final sun.misc.Unsafe m_unsafe;
        private final long m_baseAddress;

        /**
         * Constructor
         *
         * @param p_addr
         *         Native address pointing to struct
         * @param p_numNodes
         *         Max number of nodes that can be connected (connection limit)
         */
        CompletedWorkList(final long p_addr, final int p_numNodes) {
            m_numNodes = p_numNodes;

            m_unsafe = UnsafeHandler.getInstance().getUnsafe();
            m_baseAddress = p_addr;
        }

        /**
         * Get the number of nodes that have work completed
         */
        int getNumNodes() {
            return m_unsafe.getShort(m_baseAddress + IDX_NUM_ITEMS) & 0xFFFF;
        }

        /**
         * Get the number of bytes written on completion
         *
         * @param p_idx
         *         Index of the work completion list
         */
        int getNumBytesWritten(final int p_idx) {
            int tmp = m_unsafe.getInt(m_baseAddress + IDX_BYTES_WRITTEN + p_idx * SIZE_FIELD_NUM_BYTES_WRITTEN);

            if (tmp < 0) {
                throw new IllegalStateException("CompletedWorkList bytesWritten < 0: " + tmp);
            }

            return tmp;
        }

        /**
         * Get the number of fc data written on completion
         *
         * @param p_idx
         *         Index of the work completion list
         */
        int getFcDataWritten(final int p_idx) {
            return m_unsafe.getByte(m_baseAddress + IDX_FC_DATA_WRITTEN + p_idx * SIZE_FIELD_FC_DATA_WRITTEN) & 0xFF;
        }

        /**
         * Get the node id of the work completion with data sent
         *
         * @param p_idx
         *         Index of the work completion list
         */
        short getNodeId(final int p_idx) {
            if (p_idx >= m_numNodes) {
                throw new IllegalStateException("Node id index out of bounds: " + p_idx + " > " + m_numNodes);
            }

            return m_unsafe.getShort(m_baseAddress + IDX_NODE_IDS + p_idx * SIZE_FIELD_NODE_ID);
        }
    }

    /**
     * Wrapper for native struct IncomingRingBuffer
     */
    private static class IncomingRingBuffer {
        private static final int SIZE_FIELD_USED_ENTRIES = Integer.BYTES;
        private static final int SIZE_FIELD_FRONT = Integer.BYTES;
        private static final int SIZE_FIELD_BACK = Integer.BYTES;
        private static final int SIZE_FIELD_SIZE = Integer.BYTES;

        private static final int SIZE_FIELD_ENTRY_SOURCE_NODE_ID = Short.BYTES;
        private static final int SIZE_FIELD_ENTRY_FC_DATA = Byte.BYTES;
        private static final int SIZE_FIELD_ENTRY_PADDING = Byte.BYTES;
        private static final int SIZE_FIELD_DATA_LENGTH = Integer.BYTES;
        private static final int SIZE_FIELD_ENTRY_DATA = Long.BYTES;
        private static final int SIZE_FIELD_ENTRY_DATA_RAW = Long.BYTES;

        private static final int SIZE_ENTRY_STRUCT =
                SIZE_FIELD_ENTRY_SOURCE_NODE_ID + SIZE_FIELD_ENTRY_FC_DATA + SIZE_FIELD_ENTRY_PADDING +
                        SIZE_FIELD_DATA_LENGTH + SIZE_FIELD_ENTRY_DATA + SIZE_FIELD_ENTRY_DATA_RAW;

        private static final int IDX_USED_ENTRIES = 0;
        private static final int IDX_FRONT = IDX_USED_ENTRIES + SIZE_FIELD_USED_ENTRIES;
        private static final int IDX_BACK = IDX_FRONT + SIZE_FIELD_FRONT;
        private static final int IDX_SIZE = IDX_BACK + SIZE_FIELD_BACK;

        private static final int IDX_OFFSET_ENTRIES = IDX_SIZE + SIZE_FIELD_SIZE;

        private static final int IDX_ENTRY_SOURCE_NODE_ID = 0;
        private static final int IDX_ENTRY_FC_DATA = IDX_ENTRY_SOURCE_NODE_ID + SIZE_FIELD_ENTRY_SOURCE_NODE_ID;
        private static final int IDX_ENTRY_PADDING = IDX_ENTRY_FC_DATA + SIZE_FIELD_ENTRY_FC_DATA;
        private static final int IDX_ENTRY_DATA_LENGTH = IDX_ENTRY_PADDING + SIZE_FIELD_ENTRY_PADDING;
        private static final int IDX_ENTRY_PTR_DATA = IDX_ENTRY_DATA_LENGTH + SIZE_FIELD_DATA_LENGTH;
        private static final int IDX_ENTRY_PTR_DATA_RAW = IDX_ENTRY_PTR_DATA + SIZE_FIELD_ENTRY_DATA;

        //        struct IncomingRingBuffer {
        //            uint32_t m_usedEntries;
        //            uint32_t m_front;
        //            uint32_t m_back;
        //            uint32_t m_size;
        //
        //            struct Entry
        //            {
        //                con::NodeId m_sourceNodeId;
        //                uint8_t m_fcData;
        //                // ensure proper alignment
        //                uint8_t m_padding;
        //                uint32_t m_dataLength;
        //                core::IbMemReg* m_data;
        //                void* m_dataRaw;
        //            } __attribute__((__packed__)) m_entries[];
        //        } __attribute__((__packed__));
        private final sun.misc.Unsafe m_unsafe;
        private final long m_baseAddress;

        private int m_size;

        /**
         * Constructor
         *
         * @param p_addr
         *         Native address pointing to struct
         */
        IncomingRingBuffer(final long p_addr) {
            m_unsafe = UnsafeHandler.getInstance().getUnsafe();
            m_baseAddress = p_addr;

            // size won't change, cache
            m_size = m_unsafe.getInt(m_baseAddress + IDX_SIZE);

            if (m_size < 0) {
                throw new IllegalStateException("IRB, invalid value for size: " + m_size);
            }
        }

        int getUsedEntries() {
            int tmp = m_unsafe.getInt(m_baseAddress + IDX_USED_ENTRIES);

            if (tmp < 0 || tmp > m_size) {
                throw new IllegalStateException("IRB, invalid value for used entries: " + tmp);
            }

            return tmp;
        }

        /**
         * Get the size of the ring buffer
         */
        int getSize() {
            return m_size;
        }

        /**
         * Get the number of entries of the receive package
         */
        int getFront() {
            int tmp = m_unsafe.getInt(m_baseAddress + IDX_FRONT);

            if (tmp < 0 || tmp > m_size) {
                throw new IllegalStateException("IRB, invalid value fro front: " + tmp);
            }

            return tmp;
        }

        /**
         * Get the node id of a single entry
         *
         * @param p_idx
         *         Index of the entry
         */
        short getSourceNodeId(final int p_idx) {
            return m_unsafe.getShort(
                    m_baseAddress + IDX_OFFSET_ENTRIES + p_idx * SIZE_ENTRY_STRUCT + IDX_ENTRY_SOURCE_NODE_ID);
        }

        /**
         * Get the received flow control data
         *
         * @param p_idx
         *         Index of the entry
         */
        int getFcData(final int p_idx) {
            return m_unsafe.getByte(
                    m_baseAddress + IDX_OFFSET_ENTRIES + p_idx * SIZE_ENTRY_STRUCT + IDX_ENTRY_FC_DATA) & 0xFF;
        }

        /**
         * Clear the flow control data. This is only done if the fc data is processed with a higher priority when the
         * IBQ is full. There is no need to clear it in the standard processing path
         *
         * @param p_idx
         *         Index of the entry
         */
        void clearFcData(final int p_idx) {
            m_unsafe.putByte(
                    m_baseAddress + IDX_OFFSET_ENTRIES + p_idx * SIZE_ENTRY_STRUCT + IDX_ENTRY_FC_DATA, (byte) 0);
        }

        /**
         * Get the length of data received
         *
         * @param p_idx
         *         Index of the entry
         */
        int getDataLength(final int p_idx) {
            int tmp = m_unsafe.getInt(
                    m_baseAddress + IDX_OFFSET_ENTRIES + p_idx * SIZE_ENTRY_STRUCT + IDX_ENTRY_DATA_LENGTH);

            if (tmp < 0) {
                throw new IllegalStateException("IRB data length < 0: " + tmp);
            }

            return tmp;
        }

        /**
         * Get a pointer to the data received (MemReg structure)
         *
         * @param p_idx
         *         Index of the entry
         */
        public long getData(final int p_idx) {
            return m_unsafe.getLong(
                    m_baseAddress + IDX_OFFSET_ENTRIES + p_idx * SIZE_ENTRY_STRUCT + IDX_ENTRY_PTR_DATA);
        }

        /**
         * Get a (raw) pointer to the data received
         *
         * @param p_idx
         *         Index of the entry
         */
        long getDataRaw(final int p_idx) {
            return m_unsafe.getLong(
                    m_baseAddress + IDX_OFFSET_ENTRIES + p_idx * SIZE_ENTRY_STRUCT + IDX_ENTRY_PTR_DATA_RAW);
        }
    }

    /**
     * Keep track of send interests consumed by nodeId. Useful to monitor traffic distribution
     */
    private static class SendRecvTargetStats extends AbstractOperation {
        private long[] m_counter = new long[NodeID.MAX_ID];
        private long[] m_lastTime = new long[NodeID.MAX_ID];
        private long[] m_totalTime = new long[NodeID.MAX_ID];
        private long[] m_totalDataBytes = new long[NodeID.MAX_ID];
        private long[] m_totalFC = new long[NodeID.MAX_ID];

        /**
         * Constructor
         *
         * @param p_class
         *         Class that contains the operation
         * @param p_name
         *         Name for the operation
         */
        SendRecvTargetStats(final Class<?> p_class, final String p_name) {
            super(p_class, p_name);
        }

        /**
         * Record a consumed interest or if data is received
         *
         * @param p_nodeId
         *         NodeId of consumed interest or incoming data
         */
        void sendRecvCallDebug(final short p_nodeId) {
            long curTime = System.nanoTime();

            if (m_counter[p_nodeId & 0xFFFF] > 0) {
                m_totalTime[p_nodeId & 0xFFFF] += curTime - m_lastTime[p_nodeId & 0xFFFF];
            }

            m_counter[p_nodeId & 0xFFFF]++;
            m_lastTime[p_nodeId & 0xFFFF] = curTime;
        }

        void dataSentRecvDebug(final short p_nodeId, final int p_dataBytes) {
            m_totalDataBytes[p_nodeId & 0xFFFF] += p_dataBytes;
        }

        void dataFCSentRecvDebug(final short p_nodeId, final int p_fcData) {
            m_totalFC[p_nodeId & 0xFFFF] += p_fcData;
        }

        @Override
        public String dataToString(final String p_indent, final boolean p_extended) {
            StringBuilder builder = new StringBuilder();

            boolean addNewLine = false;

            for (int i = 0; i < m_counter.length; i++) {
                if (m_counter[i] > 0) {
                    if (!addNewLine) {
                        addNewLine = true;
                    } else {
                        builder.append('\n');
                    }

                    builder.append(p_indent);
                    builder.append(NodeID.toHexString((short) i));
                    builder.append(": counter ");
                    builder.append(m_counter[i]);
                    builder.append(";totalTimeMs ");
                    builder.append((double) m_totalTime[i] / 1000 / 1000);
                    builder.append(";avgTimeUs ");
                    builder.append((double) m_totalTime[i] / m_counter[i] / 1000);
                    builder.append(";totalData ");
                    builder.append((new StorageUnit(m_totalDataBytes[i], StorageUnit.BYTE)).getMBDouble());
                    builder.append(";totalFC ");
                    builder.append(m_totalFC[i]);
                }
            }

            return builder.toString();
        }

        @Override
        public String generateCSVHeader(final char p_delim) {
            return "nodeId" + p_delim + "counter" + p_delim + "totalTimeMs" + p_delim + "avgTimeUs" + p_delim +
                    "totalDataMB" + p_delim + "totalFC";
        }

        @Override
        public String toCSV(final char p_delim) {
            StringBuilder builder = new StringBuilder();

            boolean addNewLine = false;

            for (int i = 0; i < m_counter.length; i++) {
                if (m_counter[i] > 0) {
                    if (!addNewLine) {
                        addNewLine = true;
                    } else {
                        builder.append('\n');
                    }

                    builder.append(NodeID.toHexString((short) i));
                    builder.append(p_delim);
                    builder.append(m_counter[i]);
                    builder.append(p_delim);
                    builder.append((double) m_totalTime[i] / 1000 / 1000);
                    builder.append(p_delim);
                    builder.append((double) m_totalTime[i] / m_counter[i] / 1000);
                    builder.append(p_delim);
                    builder.append((new StorageUnit(m_totalDataBytes[i], StorageUnit.BYTE)).getMBDouble());
                    builder.append(p_delim);
                    builder.append(m_totalFC[i]);
                }
            }

            return builder.toString();
        }
    }
}
