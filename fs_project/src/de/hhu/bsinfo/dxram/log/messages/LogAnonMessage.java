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

package de.hhu.bsinfo.dxram.log.messages;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.backup.RangeID;
import de.hhu.bsinfo.dxram.data.ChunkAnon;
import de.hhu.bsinfo.dxutils.ByteBufferHelper;

/**
 * Message for logging an anonymous chunk on a remote node
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 20.04.2016
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.03.2017
 */
public class LogAnonMessage extends Message {

    // Attributes
    private short m_rangeID;
    // For exporting
    private ChunkAnon[] m_chunks;
    // For importing
    private int m_numberOfChunks;
    private ByteBuffer m_buffer;

    // Constructors

    /**
     * Creates an instance of LogMessage
     */
    public LogAnonMessage() {
        super();

        m_rangeID = RangeID.INVALID_ID;
        m_chunks = null;
        m_numberOfChunks = 0;
        m_buffer = null;
    }

    /**
     * Creates an instance of LogMessage
     *
     * @param p_destination
     *         the destination
     * @param p_chunks
     *         the chunks to store
     * @param p_rangeID
     *         the RangeID
     */
    public LogAnonMessage(final short p_destination, final short p_rangeID, final ChunkAnon... p_chunks) {
        super(p_destination, DXRAMMessageTypes.LOG_MESSAGES_TYPE, LogMessages.SUBTYPE_LOG_ANON_MESSAGE, true);

        m_rangeID = p_rangeID;
        m_chunks = p_chunks;
    }

    // Getters

    /**
     * Get the rangeID
     *
     * @return the rangeID
     */
    public final short getRangeID() {
        return m_rangeID;
    }

    /**
     * Get the number of data structures
     *
     * @return the number of data structures
     */
    public final int getNumberOfDataStructures() {
        return m_numberOfChunks;
    }

    /**
     * Get the message buffer
     *
     * @return the message buffer
     */
    public final ByteBuffer getMessageBuffer() {
        return m_buffer;
    }

    @Override
    protected final int getPayloadLength() {
        if (m_chunks != null) {
            int ret = Short.BYTES + Integer.BYTES;

            for (ChunkAnon chunk : m_chunks) {
                ret += Long.BYTES + chunk.sizeofObject();
            }

            return ret;
        } else {
            return m_buffer.limit() + Short.BYTES + Integer.BYTES;
        }
    }

    // Methods
    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeShort(m_rangeID);

        p_exporter.writeInt(m_chunks.length);
        for (ChunkAnon chunk : m_chunks) {
            p_exporter.writeLong(chunk.getID());
            p_exporter.exportObject(chunk);
        }
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer, final int p_payloadSize) {
        m_rangeID = p_importer.readShort(m_rangeID);
        m_numberOfChunks = p_importer.readInt(m_numberOfChunks);

        // Just copy all bytes, will be serialized into primary write buffer later
        int payloadSize = p_payloadSize - Short.BYTES - Integer.BYTES;
        if (m_buffer == null) {
            m_buffer = ByteBuffer.allocateDirect(payloadSize);
            m_buffer.order(ByteOrder.LITTLE_ENDIAN);
        }
        p_importer.readBytes(ByteBufferHelper.getDirectAddress(m_buffer), 0, payloadSize);
    }

}
