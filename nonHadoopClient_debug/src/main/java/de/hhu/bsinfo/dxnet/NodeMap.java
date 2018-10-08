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

import java.net.InetSocketAddress;

/**
 * An interface to map NodeIDs
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 20.11.2015
 */
public interface NodeMap {

    /**
     * Returns the NodeID
     *
     * @return the NodeID
     */
    short getOwnNodeID();

    /**
     * Returns the address
     *
     * @param p_nodeID
     *         the NodeID
     * @return the address
     */
    InetSocketAddress getAddress(final short p_nodeID);
}
