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

import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.backup.RangeID;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxutils.NodeID;

/**
 * Request for initialization of a backup range (which was recovered from a failed peer) on a remote node
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 20.04.2016
 */
public class InitRecoveredBackupRangeRequest extends Request {

    // Attributes
    private short m_rangeID = RangeID.INVALID_ID;
    private short m_originalRangeID = RangeID.INVALID_ID;
    private short m_originalOwner = NodeID.INVALID_ID;
    private boolean m_isNewBackupRange = false;

    // Constructors

    /**
     * Creates an instance of InitBackupRangeRequest
     */
    public InitRecoveredBackupRangeRequest() {
        super();
    }

    /**
     * Creates an instance of InitBackupRangeRequest
     *
     * @param p_destination
     *         the destination
     * @param p_rangeID
     *         the RangeID
     * @param p_isNewBackupRange
     *         whether this is a new backup range or a transferable
     */
    public InitRecoveredBackupRangeRequest(final short p_destination, final short p_rangeID, final short p_originalRangeID, final short p_originalOwner,
            final boolean p_isNewBackupRange) {
        super(p_destination, DXRAMMessageTypes.LOG_MESSAGES_TYPE, LogMessages.SUBTYPE_INIT_RECOVERED_BACKUP_RANGE_REQUEST, true);

        m_rangeID = p_rangeID;
        m_originalRangeID = p_originalRangeID;
        m_originalOwner = p_originalOwner;
        m_isNewBackupRange = p_isNewBackupRange;
    }

    // Getters

    /**
     * Get the RangeID
     *
     * @return the RangeID
     */
    public final short getRangeID() {
        return m_rangeID;
    }

    /**
     * Get the original RangeID
     *
     * @return the RangeID
     */
    public final short getOriginalRangeID() {
        return m_originalRangeID;
    }

    /**
     * Get the original owner
     *
     * @return the NodeID
     */
    public final short getOriginalOwner() {
        return m_originalOwner;
    }

    /**
     * Whether this is a new backup range or not
     *
     * @return whether this is a new backup range or not
     */
    public final boolean isNewBackupRange() {
        return m_isNewBackupRange;
    }

    @Override
    protected final int getPayloadLength() {
        return 3 * Short.BYTES + Byte.BYTES;
    }

    // Methods
    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeShort(m_rangeID);
        p_exporter.writeShort(m_originalRangeID);
        p_exporter.writeShort(m_originalOwner);
        p_exporter.writeBoolean(m_isNewBackupRange);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_rangeID = p_importer.readShort(m_rangeID);
        m_originalRangeID = p_importer.readShort(m_originalRangeID);
        m_originalOwner = p_importer.readShort(m_originalOwner);
        m_isNewBackupRange = p_importer.readBoolean(m_isNewBackupRange);
    }
}
