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

package de.hhu.bsinfo.dxram.net.messages;

/**
 * Type and list of subtypes for all network (module) messages
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 18.10.2016
 */
public final class NetworkMessages {
    public static final byte SUBTYPE_TEST_MESSAGE = 0;
    public static final byte SUBTYPE_TEST_REQUEST = 1;
    public static final byte SUBTYPE_TEST_RESPONSE = 2;
    public static final byte SUBTYPE_DEBUG_MESSAGE = 3;

    /**
     * Hidden constructor
     */
    private NetworkMessages() {
    }
}
