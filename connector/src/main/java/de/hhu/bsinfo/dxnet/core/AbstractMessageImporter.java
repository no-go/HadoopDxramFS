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

import de.hhu.bsinfo.dxutils.serialization.Importer;

/**
 * Abstraction of an Importer for network messages.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 05.07.2017
 */
public abstract class AbstractMessageImporter implements Importer {
    private long m_usedCounter;

    /**
     * Constructor
     */
    protected AbstractMessageImporter() {
    }

    public long getUsedCounter() {
        return m_usedCounter;
    }

    public void incrementUsed() {
        m_usedCounter++;
    }

    /**
     * Get current position in byte array
     *
     * @return the position
     */
    abstract int getPosition();

    /**
     * Get the number of de-serialized bytes.
     *
     * @return number of read bytes
     */
    abstract int getNumberOfReadBytes();

    /**
     * Set buffer to import from.
     *
     * @param p_addr
     *         the start address
     * @param p_size
     *         the size
     * @param p_position
     *         the offset
     */
    abstract void setBuffer(final long p_addr, final int p_size, final int p_position);

    /**
     * Set buffer to import from.
     *
     * @param p_unfinishedOperation
     *         the container for unfinished operations
     */
    abstract void setUnfinishedOperation(final UnfinishedImExporterOperation p_unfinishedOperation);

    /**
     * Set the number of read bytes. Only relevant for underflow importer to skip already finished operations.
     *
     * @param p_numberOfReadBytes
     *         the number of read bytes
     */
    abstract void setNumberOfReadBytes(final int p_numberOfReadBytes);

    /**
     * Read data into a direct byte buffer (or any other native memory area).
     *
     * @param p_byteBufferAddress
     *         Direct ByteBuffer to read into.
     * @param p_offset
     *         Offset to start in the ByteBuffer for reading into.
     * @param p_length
     *         Number of bytes to read.
     * @return Number of bytes read.
     */
    public abstract int readBytes(final long p_byteBufferAddress, final int p_offset, final int p_length);

}
