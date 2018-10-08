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

import java.util.concurrent.atomic.AtomicInteger;

import de.hhu.bsinfo.dxnet.MessageHandlers;
import de.hhu.bsinfo.dxnet.core.messages.Messages;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.UnsafeMemory;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import de.hhu.bsinfo.dxutils.stats.Time;
import de.hhu.bsinfo.dxutils.stats.TimePercentilePool;
import de.hhu.bsinfo.dxutils.stats.Value;
import de.hhu.bsinfo.dxutils.stats.ValuePool;

/**
 * Endpoint for incoming data on a connection.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 09.06.2017
 */
public abstract class AbstractPipeIn {
    public static final TimePercentilePool SOP_REQ_RESP_RTT =
            new TimePercentilePool(AbstractPipeIn.class, "ReqRespRTT");
    public static final ValuePool SOP_REQ_RESP_RTT_VAL = new ValuePool(AbstractPipeIn.class, "ReqRespRTTVal");
    private static final Time SOP_PROCESS = new Time(AbstractPipeIn.class, "ProcessBuffer");
    private static final Value SOP_FULFILL = new Value(AbstractPipeIn.class, "FulfillRequest");
    private static final Time SOP_WAIT_SLOT = new Time(AbstractPipeIn.class, "WaitSlot");

    static {
        StatisticsManager.get().registerOperation(AbstractPipeIn.class, SOP_REQ_RESP_RTT);
        StatisticsManager.get().registerOperation(AbstractPipeIn.class, SOP_REQ_RESP_RTT_VAL);
        StatisticsManager.get().registerOperation(AbstractPipeIn.class, SOP_PROCESS);
        StatisticsManager.get().registerOperation(AbstractPipeIn.class, SOP_FULFILL);
        StatisticsManager.get().registerOperation(AbstractPipeIn.class, SOP_WAIT_SLOT);
    }

    private static final int BUFFER_SLOTS = 8;

    private final short m_ownNodeID;
    private final short m_destinationNodeID;
    private volatile boolean m_isConnected;
    private final AbstractFlowControl m_flowControl;

    private final LocalMessageHeaderPool m_messageHeaderPool; // Used by MessageCreationCoordinator, only
    private final MessageHandlers m_messageHandlers;
    private final int m_messageHandlerPoolSize;
    private final MessageDirectory m_messageDirectory;
    private final RequestMap m_requestMap;

    private int m_slotPosition;
    private AtomicInteger[] m_slotMessageCounters;
    private BufferPool.DirectBufferWrapper[] m_slotBufferWrappers;
    private long[] m_slotBufferHandles;
    private UnfinishedImExporterOperation[] m_slotUnfinishedOperations;

    private long m_receivedMessages;

    private final MessageImporterCollection m_importers;
    private UnfinishedImExporterOperation m_unfinishedOperation;
    private final UnfinishedImExporterOperation m_dummyOperation;

    private final boolean m_benchmarkMode;

    /**
     * Constructor
     *
     * @param p_ownNodeId
     *         Node id of the current instance (receiver)
     * @param p_destinationNodeId
     *         Node id of the destination to receive data from
     * @param p_flowControl
     *         FlowControl instance of the connection
     * @param p_messageDirectory
     *         MessageDirectory instance
     * @param p_requestMap
     *         RequestMap instance
     * @param p_messageHandlers
     *         MessageHandlers instance
     * @param p_benchmarkMode
     *         True to enable benchmark mode and record all RTT values to calculate percentile
     */
    protected AbstractPipeIn(final short p_ownNodeId, final short p_destinationNodeId,
            final LocalMessageHeaderPool p_messageHeaderPool, final AbstractFlowControl p_flowControl,
            final MessageDirectory p_messageDirectory, final RequestMap p_requestMap,
            final MessageHandlers p_messageHandlers, final boolean p_benchmarkMode) {
        m_ownNodeID = p_ownNodeId;
        m_destinationNodeID = p_destinationNodeId;
        m_flowControl = p_flowControl;

        m_importers = new MessageImporterCollection();

        m_messageHeaderPool = p_messageHeaderPool;
        m_messageHandlers = p_messageHandlers;
        m_messageHandlerPoolSize = MessageHandlers.getPoolSize();
        m_messageDirectory = p_messageDirectory;
        m_requestMap = p_requestMap;

        m_slotPosition = 0;
        m_slotMessageCounters = new AtomicInteger[BUFFER_SLOTS];
        m_slotBufferWrappers = new BufferPool.DirectBufferWrapper[BUFFER_SLOTS];
        m_slotBufferHandles = new long[BUFFER_SLOTS];
        // We need one unfinished operation more to avoid waiting for the next slot being finished by message handlers
        // (the unfinished operation is filled in slot, but the message is continued in next slot)
        m_slotUnfinishedOperations = new UnfinishedImExporterOperation[BUFFER_SLOTS + 1];

        for (int i = 0; i < BUFFER_SLOTS; i++) {
            m_slotMessageCounters[i] = new AtomicInteger(0);
        }

        for (int i = 0; i <= BUFFER_SLOTS; i++) {
            m_slotUnfinishedOperations[i] = new UnfinishedImExporterOperation();
        }

        m_unfinishedOperation = m_slotUnfinishedOperations[m_slotPosition];
        m_dummyOperation = new UnfinishedImExporterOperation();

        m_benchmarkMode = p_benchmarkMode;
    }

    /**
     * Get the node id of the destination to receive data from
     */
    public short getDestinationNodeID() {
        return m_destinationNodeID;
    }

    /**
     * Check if the pipe is connected to the remote
     */
    public boolean isConnected() {
        return m_isConnected;
    }

    /**
     * Set the pipe connected
     */
    public void setConnected(final boolean p_connected) {
        m_isConnected = p_connected;
    }

    @Override
    public String toString() {
        return "PipeIn[m_ownNodeID " + NodeID.toHexString(m_ownNodeID) + ", m_destinationNodeID " +
                NodeID.toHexString(m_destinationNodeID) + ", m_flowControl " + m_flowControl + ", m_receivedMessages " +
                m_receivedMessages + ']';
    }

    /**
     * Return a processed buffer (to a buffer pool)
     *
     * @param p_obj
     *         Buffer object to return (implementation dependant)
     * @param p_bufferHandle
     *         Buffer handle to return (implementation dependant)
     */
    public abstract void returnProcessedBuffer(final Object p_obj, final long p_bufferHandle);

    /**
     * Check if the pipe is opened
     */
    public abstract boolean isOpen();

    /**
     * Get the FlowControl instance connected to the pipe
     */
    protected AbstractFlowControl getFlowControl() {
        return m_flowControl;
    }

    /**
     * Get the node id of the current instance
     */
    short getOwnNodeID() {
        return m_ownNodeID;
    }

    /**
     * Process an incoming buffer.
     *
     * @param p_incomingBuffer
     *         the incoming buffer to process
     */
    void processBuffer(final IncomingBufferQueue.IncomingBuffer p_incomingBuffer) throws NetworkException {
        final long address = p_incomingBuffer.getBufferAddress();
        final int bytesAvailable = p_incomingBuffer.getBufferSize();
        final int slot = m_slotPosition % BUFFER_SLOTS;
        final int slotUnfinishedOperation = m_slotPosition % (BUFFER_SLOTS + 1);
        int currentPosition = 0;
        MessageHeader messageHeader = m_unfinishedOperation.isEmpty() ? m_messageHeaderPool.getHeader() :
                m_unfinishedOperation.getMessageHeader();

        SOP_PROCESS.startDebug();

        m_flowControl.dataReceived(bytesAvailable);

        AtomicInteger messageCounter = enterBufferSlot(p_incomingBuffer, slot);

        if (m_unfinishedOperation.isEmpty()) {
            // Switch to unfinished operation from current slot if last message of last buffer could be completed
            m_unfinishedOperation = m_slotUnfinishedOperations[slotUnfinishedOperation];
            m_unfinishedOperation.reset();
        }

        while (true) {
            /* Read header */

            if (m_unfinishedOperation.isEmpty() || !m_unfinishedOperation.wasMessageCreated()) {
                // Skip reading header only if message payload could not be read entirely (only relevant for first
                // iteration)
                if (currentPosition + Message.HEADER_SIZE - m_unfinishedOperation.getBytesCopied() > bytesAvailable) {
                    // End of current data stream in importer, incomplete header
                    readHeader(messageHeader, currentPosition, address, bytesAvailable, m_unfinishedOperation,
                            m_importers);

                    if (!m_unfinishedOperation.isEmpty()) {
                        // Overflow + underflow: transfer state of unfinished operation to new unfinished operation
                        UnfinishedImExporterOperation tmp = m_unfinishedOperation;

                        // Switch to unfinished operation from current slot as soon as last message header of last
                        // buffer is completed
                        m_unfinishedOperation = m_slotUnfinishedOperations[slotUnfinishedOperation];
                        m_unfinishedOperation.reset();

                        m_unfinishedOperation.transfer(tmp);
                    }

                    // Store the number of bytes copied and incomplete message header to continue with next buffer
                    m_unfinishedOperation.incrementBytesCopied(bytesAvailable - currentPosition);
                    m_unfinishedOperation.setMessageHeader(messageHeader);

                    break;
                }

                readHeader(messageHeader, currentPosition, address, bytesAvailable, m_unfinishedOperation, m_importers);

                if (m_unfinishedOperation.isEmpty()) {
                    currentPosition += Message.HEADER_SIZE;
                } else {
                    // The header was not finished with last buffer but with current one -> increase position by read
                    // bytes in current buffer, only
                    currentPosition += Message.HEADER_SIZE - m_unfinishedOperation.getBytesCopied();
                    // Switch to unfinished operation from current slot as soon as last message header of last buffer
                    // is completed
                    m_unfinishedOperation = m_slotUnfinishedOperations[slotUnfinishedOperation];
                    m_unfinishedOperation.reset();
                }
            }

            // Check type and subtype (must not be both 0)
            byte type = messageHeader.getType();
            byte subtype = messageHeader.getSubtype();

            if (!m_messageDirectory.contains(type, subtype)) {
                StringBuilder builder = new StringBuilder();
                int len = bytesAvailable;

                if (len > 1024) {
                    len = 1024;
                }

                for (int i = currentPosition; i < currentPosition + len; i++) {
                    builder.append(Integer.toHexString(UnsafeMemory.readByte(address + i) & 0xFF));
                    builder.append(' ');
                }

                if (type == Messages.DEFAULT_MESSAGES_TYPE && subtype == Messages.SUBTYPE_INVALID_MESSAGE) {
                    throw new NetworkException("Invalid message type 0, subtype 0, most likely corrupted " +
                            "message/buffer. Current message header: " + messageHeader + "\nBuffer section (first " +
                            "index is start of message header): " + builder + "\nImporterCollectionState:\n" +
                            m_importers);
                } else {
                    throw new NetworkException("Invalid message type " + type + ", subtype " + subtype +
                            ", not registered in message directory. Current message header: " + messageHeader +
                            "\nBuffer section (first index is start of message header): " + builder +
                            "\nImporterCollectionState:\n" + m_importers);
                }
            }

            // Ignore network test messages (e.g. ping after response delay). Default messages do not have a payload.
            if (messageHeader.getType() == Messages.DEFAULT_MESSAGES_TYPE &&
                    messageHeader.getSubtype() == Messages.SUBTYPE_DEFAULT_MESSAGE) {
                continue;
            }

            // Create and read message. Delegated to message handlers if message is included entirely in current
            // incoming buffer.

            int payloadSize = messageHeader.getPayloadSize();
            if (currentPosition + payloadSize - m_unfinishedOperation.getBytesCopied() > bytesAvailable) {
                // End of current data stream in importer, incomplete message
                // Last message is separated -> take over creation to provide message reference for next buffer

                Message message = createAndImportMessage(messageHeader, address, currentPosition, bytesAvailable,
                        m_unfinishedOperation, m_importers, m_messageHeaderPool, slot);

                if (!m_unfinishedOperation.isEmpty()) {
                    // Overflow + underflow: transfer state of unfinished operation to new unfinished operation
                    UnfinishedImExporterOperation tmp = m_unfinishedOperation;

                    // Switch to unfinished operation from current slot as soon as last message of last
                    // buffer is completed
                    m_unfinishedOperation = m_slotUnfinishedOperations[slotUnfinishedOperation];
                    m_unfinishedOperation.reset();

                    m_unfinishedOperation.transfer(tmp);
                }

                // Store information about this message to continue with next buffer
                m_unfinishedOperation.incrementBytesCopied(bytesAvailable - currentPosition);
                m_unfinishedOperation.setMessageHeader(messageHeader);
                m_unfinishedOperation.setMessage(message);

                break;
            }

            // Delegate to message handlers

            if (m_unfinishedOperation.isEmpty()) {
                messageHeader
                        .setMessageInformation(this, m_dummyOperation, address, currentPosition, bytesAvailable, slot);
                currentPosition += payloadSize;
            } else {
                // The message was not finished with last buffer but with current one
                messageHeader
                        .setMessageInformation(this, m_unfinishedOperation, address, currentPosition, bytesAvailable,
                                slot);
                currentPosition += payloadSize - m_unfinishedOperation.getBytesCopied();
                // Switch to unfinished operation from current slot as soon as last message of last buffer is completed
                m_unfinishedOperation = m_slotUnfinishedOperations[slotUnfinishedOperation];
                m_unfinishedOperation.reset();
            }

            if (m_messageHandlers.newHeader(messageHeader)) {
                messageCounter.addAndGet(m_messageHandlerPoolSize);
            }
            m_receivedMessages++;

            if (bytesAvailable == currentPosition) {
                // Buffer fully processed
                break;
            } else {
                // Prepare next iteration by getting a new message header
                messageHeader = m_messageHeaderPool.getHeader();
            }
        }

        leaveBufferSlot(messageCounter, p_incomingBuffer);

        SOP_PROCESS.stopDebug();
    }

    /**
     * Create a message and de-serialize payload. Might be interrupted if buffer is too small.
     *
     * @param p_header
     *         the already de-serialized message header
     * @param p_address
     *         the buffer address in native memory
     * @param p_currentPosition
     *         the current position in buffer
     * @param p_bytesAvailable
     *         the size of the buffer
     * @param p_unfinishedOperation
     *         the unfinished operation of previous slot (relevant for first message in slot, only)
     * @param p_importerCollection
     *         the importer collection
     * @param p_messageHeaderPool
     *         the message header pool
     * @param p_slot
     *         the slot index
     * @return the completed message or null if message is a response
     * @throws NetworkException
     *         if de-serialization failed
     */
    Message createAndImportMessage(final MessageHeader p_header, final long p_address, final int p_currentPosition,
            final int p_bytesAvailable, final UnfinishedImExporterOperation p_unfinishedOperation,
            final MessageImporterCollection p_importerCollection, final LocalMessageHeaderPool p_messageHeaderPool,
            final int p_slot) throws NetworkException {
        Message message;
        Request request;

        if (p_header.isAborted()) {
            // This is a response which is split to several buffers and is too late to be used
            if (p_unfinishedOperation.getBytesCopied() + p_bytesAvailable >= p_header.getPayloadSize()) {
                finishHeader(p_header, p_slot, p_messageHeaderPool);
            } else {
                // We do not have to return buffer (executed by MCC) and header (message still incomplete) here!
            }

            return p_unfinishedOperation.getMessage();
        }

        if (p_unfinishedOperation.isEmpty() || !p_unfinishedOperation.wasMessageCreated()) {
            // Create a new message
            message = createMessage(p_header);

            // Important: set corresponding request BEFORE readPayload. The call might use the request
            if (message.isResponse()) {
                // hack:
                // to avoid copying data multiple times, some responses use the same objects provided
                // with the request to directly write the data to them instead of creating a temporary
                // object in the response, de-serializing the data and then copying from the temporary object
                // to the object that should receive the data in the first place. (example DXRAM: get request/response)
                // This is only possible, if we have a reference to the original request within the response
                // while reading from the network byte buffer. But in this low level stage, we (usually) don't have
                // access to requests/responses. So we exploit the request map to get our corresponding request
                // before de-serializing the network buffer for every request.
                Response response = (Response) message;
                request = m_requestMap.getRequest(response);

                if (request != null) {
                    response.setCorrespondingRequest(request);
                }
            }
        } else {
            // Continue with partly de-serialized message
            message = p_unfinishedOperation.getMessage();
        }

        if (message.isResponse()) {
            request = m_requestMap.getRequest((Response) message);

            // abort if request timed out but response arrived very late
            // which results in not finding the corresponding request anymore
            // just drop the response because the data for it is already skipped
            if (request == null) {
                // cleanup
                if (p_unfinishedOperation.getBytesCopied() + p_bytesAvailable >= p_header.getPayloadSize()) {
                    finishHeader(p_header, p_slot, p_messageHeaderPool);
                } else {
                    // We do not have to return buffer (executed by MCC) and header (message still incomplete) here!
                }

                return message;
            }
        }

        if (!readPayload(p_currentPosition, message, p_address, p_bytesAvailable, p_header.getPayloadSize(),
                p_unfinishedOperation, p_importerCollection)) {
            // Message could not be completely de-serialized
            return message;
        }

        if (message.getPayloadLength() != p_header.getPayloadSize()) {
            throw new NetworkException("Read message size in header differs from calculated size. Size in header " +
                    (p_header.getPayloadSize() + Message.HEADER_SIZE) + " bytes, expected " +
                    (message.getPayloadLength() + Message.HEADER_SIZE) +
                    " bytes (including header). Check getPayloadLength method of message type " +
                    message.getClass().getSimpleName());
        }

        finishHeader(p_header, p_slot, p_messageHeaderPool);

        if (message.isResponse()) {
            long timeReceiveResponse;
            Response response = (Response) message;

            response.setSendReceiveTimestamp(System.nanoTime());
            timeReceiveResponse = response.getSendReceiveTimestamp();
            request = m_requestMap.remove(response.getRequestID());

            if (request != null) {
                // Must be executed prior to fulfill()!
                // never remove these statistics (even on performance build type). required to determine
                // RTT on benchmarks
                if (m_benchmarkMode) {
                    SOP_REQ_RESP_RTT.recordPerf(timeReceiveResponse - request.getSendReceiveTimestamp());
                } else {
                    SOP_REQ_RESP_RTT_VAL.addPerf(timeReceiveResponse - request.getSendReceiveTimestamp());
                }

                SOP_FULFILL.incDebug();

                request.fulfill(response);
            }

            return null;
        } else {
            return message;
        }
    }

    /**
     * Update metadata after processing message/header.
     *
     * @param p_header
     *         the message header
     * @param p_slot
     *         the slot
     * @param p_messageHeaderPool
     *         the local message header pool
     */
    void finishHeader(final MessageHeader p_header, final int p_slot,
            final LocalMessageHeaderPool p_messageHeaderPool) {
        updateBufferSlot(p_slot);
        p_messageHeaderPool.returnHeader(p_header);
    }

    /**
     * Enter a new buffer slot. Is called by MessageCreationCoordinator.
     *
     * @param p_incomingBuffer
     *         the new incoming buffer
     * @param p_slot
     *         the slot to enter
     * @return the message counter used for this slot
     */
    private AtomicInteger enterBufferSlot(IncomingBufferQueue.IncomingBuffer p_incomingBuffer, final int p_slot) {
        if (m_slotMessageCounters[p_slot].get() > 0) {
            SOP_WAIT_SLOT.start();

            do {
                Thread.yield();//LockSupport.parkNanos(1);
            } while (m_slotMessageCounters[p_slot].get() > 0);

            SOP_WAIT_SLOT.stop();
        }

        // Get slot message counter
        AtomicInteger messageCounter = m_slotMessageCounters[p_slot];
        // Avoid returning buffer too early by message handler; message counter is decremented at the end
        messageCounter.set(2 * m_messageHandlerPoolSize);
        // Set buffer (NIO/Loopback) and buffer handle (IB) for message handlers
        m_slotBufferWrappers[p_slot] = p_incomingBuffer.getDirectBuffer();
        m_slotBufferHandles[p_slot] = p_incomingBuffer.getBufferHandle();

        return messageCounter;
    }

    /**
     * Leave buffer slot after processing all messages (reading message
     * headers and dispatching creation). Is called by MessageCreationCoordinator.
     *
     * @param p_messageCounter
     *         the slot's message counter
     * @param p_incomingBuffer
     *         the processed incoming buffer
     */
    private void leaveBufferSlot(final AtomicInteger p_messageCounter,
            final IncomingBufferQueue.IncomingBuffer p_incomingBuffer) {
        // Message counter was incremented during entering the slot -> decrement now as all message headers were read
        int counter = p_messageCounter.addAndGet(-(2 * m_messageHandlerPoolSize - m_messageHandlers.pushLeftHeaders()));
        if (counter == 0) {
            // Message handlers created and de-serialized all messages for this buffer already -> return incoming buffer
            returnProcessedBuffer(p_incomingBuffer.getDirectBuffer(), p_incomingBuffer.getBufferHandle());
        }
        // Increment slot number
        m_slotPosition = m_slotPosition + 1 & 0x7FFFFFFF;
    }

    /**
     * Update buffer slot: decrement message counter and return
     * incoming buffer to buffer pool if counter is 0. Is called by MessageHandlers.
     *
     * @param p_slotIndex
     *         the slot index
     */
    private void updateBufferSlot(final int p_slotIndex) {
        // Get buffer and buffer handle before decrementing counter (race condition with message creator otherwise)
        BufferPool.DirectBufferWrapper buffer = m_slotBufferWrappers[p_slotIndex];
        long bufferHandle = m_slotBufferHandles[p_slotIndex];

        // Decrement message counter for this slot and return incoming buffer if all
        // messages have been created and de-serialized
        int counter = m_slotMessageCounters[p_slotIndex].decrementAndGet();
        if (counter == 0) {
            returnProcessedBuffer(buffer, bufferHandle);
        }
    }

    /**
     * Create a message from a given buffer
     *
     * @param p_header
     *         the message header
     * @return New message instance created from the buffer(s)
     * @throws NetworkException
     *         If creating/reading/deserializing the message failed
     */
    private Message createMessage(final MessageHeader p_header) throws NetworkException {
        Message ret;
        byte type = p_header.getType();
        byte subtype = p_header.getSubtype();

        try {
            ret = m_messageDirectory.getInstance(type, subtype);
        } catch (final Exception e) {
            throw new NetworkException("Unable to create message of type " + type + ", subtype " + subtype +
                    ". Type is missing in message directory. Current message header " + p_header, e);
        }

        ret.initialize(p_header, m_ownNodeID, m_destinationNodeID);

        return ret;
    }

    /**
     * Read the header of a message from a buffer
     *
     * @param p_header
     *         the message header object
     * @param p_currentPosition
     *         the position in current header
     * @param p_address
     *         (Unsafe) address of the buffer
     * @param p_bytesAvailable
     *         Number of bytes available in the buffer
     * @param p_unfinishedOperation
     *         the unfinished operation
     * @param p_importerCollection
     *         the importer collection
     */
    private static void readHeader(final MessageHeader p_header, final int p_currentPosition, final long p_address,
            final int p_bytesAvailable, final UnfinishedImExporterOperation p_unfinishedOperation,
            final MessageImporterCollection p_importerCollection) {
        AbstractMessageImporter importer = p_importerCollection
                .getImporter(Message.HEADER_SIZE, p_address, p_currentPosition, p_bytesAvailable,
                        p_unfinishedOperation);

        try {
            importer.importObject(p_header);
        } catch (final ArrayIndexOutOfBoundsException e) {
            // Header is incomplete continue later
        }
    }

    /**
     * Read the payload of a message from a buffer
     *
     * @param p_currentPosition
     *         the position in current buffer
     * @param p_message
     *         the message
     * @param p_address
     *         (Unsafe) address of a buffer
     * @param p_bytesAvailable
     *         Number of bytes available in the buffer
     * @param p_bytesToRead
     *         Number of bytes to read from the buffer
     * @param p_unfinishedOperation
     *         the unfinished operation
     * @param p_importerCollection
     *         the importer collection (thread-local)
     * @return reading payload was completed
     * @throws NetworkException
     *         if de-serialization failed
     */
    private static boolean readPayload(final int p_currentPosition, final Message p_message, final long p_address,
            final int p_bytesAvailable, final int p_bytesToRead,
            final UnfinishedImExporterOperation p_unfinishedOperation,
            final MessageImporterCollection p_importerCollection) throws NetworkException {
        AbstractMessageImporter importer = p_importerCollection
                .getImporter(p_bytesToRead, p_address, p_currentPosition, p_bytesAvailable, p_unfinishedOperation);

        try {
            p_message.readPayload(importer, p_bytesToRead);
        } catch (final ArrayIndexOutOfBoundsException e) {
            // Message is incomplete -> continue later
            return false;
        }

        int readBytes = importer.getNumberOfReadBytes();
        if (readBytes < p_bytesToRead) {
            throw new NetworkException(
                    "Message deserialization finished too early (without overflow): " + p_bytesToRead +
                            " (payload size) > " + readBytes + " (deserialized bytes), current Message: " +
                            p_message.getClass().getName() + ", importer type: " + importer.getClass().getSimpleName() +
                            ", importer detail: " + importer + "\nImporterCollectionState:\n" + p_importerCollection);
        }

        return true;
    }
}
