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

import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxutils.NodeID;

/**
 * Notify About New Predecessor Message
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 06.09.2012
 */
public class NotifyAboutNewPredecessorMessage extends Message {

    // Attributes
    private short m_newPredecessor;

    // Constructors

    /**
     * Creates an instance of NotifyAboutNewPredecessorMessage
     */
    public NotifyAboutNewPredecessorMessage() {
        super();

        m_newPredecessor = NodeID.INVALID_ID;
    }

    /**
     * Creates an instance of NotifyAboutNewPredecessorMessage
     *
     * @param p_destination
     *         the destination
     * @param p_newPredecessor
     *         the new predecessor
     */
    public NotifyAboutNewPredecessorMessage(final short p_destination, final short p_newPredecessor) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_NOTIFY_ABOUT_NEW_PREDECESSOR_MESSAGE);

        assert p_newPredecessor != NodeID.INVALID_ID;

        m_newPredecessor = p_newPredecessor;
    }

    // Getters

    /**
     * Get the new predecessor
     *
     * @return the NodeID
     */
    public final short getNewPredecessor() {
        return m_newPredecessor;
    }

    @Override
    protected final int getPayloadLength() {
        return Short.BYTES;
    }

    // Methods
    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeShort(m_newPredecessor);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_newPredecessor = p_importer.readShort(m_newPredecessor);
    }

}
