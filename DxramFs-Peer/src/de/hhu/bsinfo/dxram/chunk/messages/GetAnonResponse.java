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

package de.hhu.bsinfo.dxram.chunk.messages;

import de.hhu.bsinfo.dxram.data.ChunkAnon;
import de.hhu.bsinfo.dxram.data.ChunkState;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Response;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Response to a GetAnonRequest
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 30.03.2017
 */
public class GetAnonResponse extends Response {

    // The data of the chunk buffer objects here is used when sending the response only
    // when the response is received, the chunk objects from the request are
    // used to directly write the data to them and avoiding further copying
    private byte[][] m_dataChunks;
    private int m_totalSuccessful;

    /**
     * Creates an instance of GetAnonResponse.
     * This constructor is used when receiving this message.
     */
    public GetAnonResponse() {
        super();
    }

    /**
     * Creates an instance of GetAnonResponse.
     * This constructor is used when sending this message.
     * Make sure to include all data of the chunks from the request in the correct order. If a chunk does
     * not exist, set the byte[] at that index to null
     *
     * @param p_request
     *         the corresponding GetAnonRequest
     * @param p_dataChunks
     *         Array of byte arrays with chunk data read from the memory. If a chunk does not exist, the byte[] is null
     * @param p_totalSuccessful
     *         Number of total successful get operations
     */
    public GetAnonResponse(final GetAnonRequest p_request, final byte[][] p_dataChunks, final int p_totalSuccessful) {
        super(p_request, ChunkMessages.SUBTYPE_GET_ANON_RESPONSE);
        m_totalSuccessful = p_totalSuccessful;
        m_dataChunks = p_dataChunks;
    }

    /**
     * Get the total number of successful chunk gets
     *
     * @return Total number of successful chunk gets
     */
    public int getTotalSuccessful() {
        return m_totalSuccessful;
    }

    @Override
    protected final int getPayloadLength() {
        int size = ObjectSizeUtil.sizeofCompactedNumber(m_totalSuccessful);

        // when writing payload
        if (m_dataChunks != null) {
            size += m_dataChunks.length * Byte.BYTES;

            for (int i = 0; i < m_dataChunks.length; i++) {
                if (m_dataChunks[i] != null) {
                    size += ObjectSizeUtil.sizeofByteArray(m_dataChunks[i]);
                }
            }
        } else {
            // after reading message payload to request data structures
            GetAnonRequest request = (GetAnonRequest) getCorrespondingRequest();

            size += request.getChunks().length * Byte.BYTES;

            for (int i = 0; i < request.getChunks().length; i++) {
                if (request.getChunks()[i] != null && request.getChunks()[i].getState() == ChunkState.OK) {
                    size += request.getChunks()[i].sizeofObject();
                }
            }
        }

        return size;
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeCompactNumber(m_totalSuccessful);
        for (int i = 0; i < m_dataChunks.length; i++) {
            if (m_dataChunks[i] == null) {
                // indicate no data available
                p_exporter.writeByte((byte) 0);
            } else {
                p_exporter.writeByte((byte) 1);
                p_exporter.writeByteArray(m_dataChunks[i]);
            }
        }
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_totalSuccessful = p_importer.readCompactNumber(m_totalSuccessful);

        // read the payload from the buffer and write it directly into
        // the chunk buffer objects provided by the request to avoid further copying of data
        GetAnonRequest request = (GetAnonRequest) getCorrespondingRequest();

        for (ChunkAnon chunk : request.getChunks()) {
            if (p_importer.readByte((byte) (chunk.getState() == ChunkState.DOES_NOT_EXIST ? 0 : 1)) == 1) {
                p_importer.importObject(chunk);
                chunk.setState(ChunkState.OK);
            } else {
                chunk.setState(ChunkState.DOES_NOT_EXIST);
            }
        }
    }

}
