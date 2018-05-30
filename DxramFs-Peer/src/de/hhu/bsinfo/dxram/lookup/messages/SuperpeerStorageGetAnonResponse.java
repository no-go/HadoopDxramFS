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

package de.hhu.bsinfo.dxram.lookup.messages;

import de.hhu.bsinfo.dxram.data.ChunkState;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Response;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Response to the get request.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 04.042017
 */
public class SuperpeerStorageGetAnonResponse extends Response {
    // The data of the chunk buffer object here is used when sending the response only
    // when the response is received, the chunk objects from the request are
    // used to directly write the data to it to avoid further copying
    private byte[] m_data;
    private byte m_status;

    /**
     * Creates an instance of SuperpeerStorageGetAnonResponse.
     * This constructor is used when receiving this message.
     */
    public SuperpeerStorageGetAnonResponse() {
        super();
    }

    /**
     * Creates an instance of SuperpeerStorageGetAnonResponse.
     * This constructor is used when sending this message.
     *
     * @param p_request
     *         the corresponding GetRequest
     * @param p_data
     *         Data read from memory
     */
    public SuperpeerStorageGetAnonResponse(final SuperpeerStorageGetAnonRequest p_request, final byte[] p_data, final byte p_status) {
        super(p_request, LookupMessages.SUBTYPE_SUPERPEER_STORAGE_GET_ANON_RESPONSE);
        m_data = p_data;
        m_status = p_status;
    }

    /**
     * Get the status
     *
     * @return the status
     */
    public int getStatus() {
        return m_status;
    }

    @Override
    protected final int getPayloadLength() {
        if (m_data != null) {
            return Byte.BYTES + ObjectSizeUtil.sizeofByteArray(m_data);
        } else {
            SuperpeerStorageGetAnonRequest request = (SuperpeerStorageGetAnonRequest) getCorrespondingRequest();
            return Byte.BYTES + request.getDataStructure().sizeofObject();
        }
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeByte(m_status);
        if (m_data != null) {
            p_exporter.writeByteArray(m_data);
        }
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        // read the payload from the buffer and write it directly into
        // the data structure provided by the request to avoid further copying of data
        SuperpeerStorageGetAnonRequest request = (SuperpeerStorageGetAnonRequest) getCorrespondingRequest();

        m_status = p_importer.readByte(m_status);
        if (m_status == 0) {
            p_importer.importObject(request.getDataStructure());
            request.getDataStructure().setState(ChunkState.OK);
        } else {
            request.getDataStructure().setState(ChunkState.DOES_NOT_EXIST);
        }
    }
}
