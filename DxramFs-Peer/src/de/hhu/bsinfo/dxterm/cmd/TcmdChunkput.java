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

package de.hhu.bsinfo.dxterm.cmd;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import de.hhu.bsinfo.dxram.chunk.ChunkAnonService;
import de.hhu.bsinfo.dxram.data.ChunkAnon;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxterm.AbstractTerminalCommand;
import de.hhu.bsinfo.dxterm.TerminalCommandString;
import de.hhu.bsinfo.dxterm.TerminalServerStdin;
import de.hhu.bsinfo.dxterm.TerminalServerStdout;
import de.hhu.bsinfo.dxterm.TerminalServiceAccessor;
import de.hhu.bsinfo.dxutils.NodeID;

/**
 * Get a chunk from a storage
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public class TcmdChunkput extends AbstractTerminalCommand {
    public TcmdChunkput() {
        super("chunkput");
    }

    @Override
    public String getHelp() {
        return "Put data to the specified chunk. Either use a full cid or separate nid + lid to specify the chunk id. " +
                "If no offset is specified, the whole chunk is overwritten with the new data. " +
                "Otherwise the data is inserted at the starting offset with its length. " + "If the specified data is too long it will be truncated\n" +
                "Usage (1): chunkput <cid> <data> [offset] [type]\n" + "Usage (2): chunkput <nid> <lid> <data> [offset] [type]\n" +
                "  cid: Full chunk ID of the chunk to put data to\n" + "  nid: (Or) separate local id part of chunk to put the data to" +
                "  lid: (In combination with) separate node id part of the chunk to put the data to\n" +
                "  data: Data to store (format has to match type parameter)\n" +
                "  type: Type of the data to store (\"str\", \"byte\", \"short\", \"int\", \"long\", \"hex\"), defaults to \"str\"\n" +
                "  offset: Offset within the existing to store the new data to. -1 to override existing data, defaults to 0";
    }

    @Override
    public void exec(final TerminalCommandString p_cmd, final TerminalServerStdout p_stdout, final TerminalServerStdin p_stdin,
            final TerminalServiceAccessor p_services) {
        long cid;
        String data;
        String type;
        int offset;

        if (p_cmd.isArgChunkID(0)) {
            cid = p_cmd.getArgChunkId(0, ChunkID.INVALID_ID);
            data = p_cmd.getArgString(1, null);
            type = p_cmd.getArgString(2, "str").toLowerCase();
            offset = p_cmd.getArgInt(3, -1);
        } else {
            short nid = p_cmd.getArgNodeId(0, NodeID.INVALID_ID);
            long lid = p_cmd.getArgLocalId(1, ChunkID.INVALID_ID);

            if (nid == ChunkID.INVALID_ID) {
                p_stdout.printlnErr("No nid specified");
                return;
            }

            if (lid == ChunkID.INVALID_ID) {
                p_stdout.printlnErr("No lid specified");
                return;
            }

            cid = ChunkID.getChunkID(nid, lid);

            data = p_cmd.getArgString(2, null);
            type = p_cmd.getArgString(3, "str").toLowerCase();
            offset = p_cmd.getArgInt(4, -1);
        }

        if (cid == ChunkID.INVALID_ID) {
            p_stdout.printlnErr("No cid specified");
            return;
        }

        if (data == null) {
            p_stdout.printlnErr("No data specified");
            return;
        }

        // don't allow put of index chunk
        if (ChunkID.getLocalID(cid) == 0) {
            p_stdout.printlnErr("Put of index chunk is not allowed");
            return;
        }

        ChunkAnonService chunkService = p_services.getService(ChunkAnonService.class);

        ChunkAnon[] chunks = new ChunkAnon[1];

        if (chunkService.get(chunks, cid) != 1) {
            p_stdout.printflnErr("Getting chunk 0x%X failed: %s", cid, chunks[0].getState());
            return;
        }

        ChunkAnon chunk = chunks[0];
        if (offset == -1) {
            // wipe chunk
            for (int i = 0; i < chunk.getDataSize(); i++) {
                chunk.getData()[i] = 0;
            }
            offset = 0;
        }

        if (offset > chunk.sizeofObject()) {
            offset = chunk.sizeofObject();
        }

        ByteBuffer byteBuffer = ByteBuffer.wrap(chunk.getData());
        byteBuffer.position(offset);

        switch (type) {
            case "str":
                byte[] bytes = data.getBytes(java.nio.charset.StandardCharsets.US_ASCII);

                try {
                    int size = byteBuffer.capacity() - byteBuffer.position();
                    if (bytes.length < size) {
                        size = bytes.length;
                    }
                    byteBuffer.put(bytes, 0, size);
                } catch (final Exception ignored) {
                    // that's fine, trunc data
                }

                break;

            case "byte":
                try {
                    byteBuffer.put((byte) (Integer.parseInt(data) & 0xFF));
                } catch (final Exception ignored) {
                    // that's fine, trunc data
                }

                break;

            case "short":
                try {
                    byteBuffer.putShort((short) (Integer.parseInt(data) & 0xFFFF));
                } catch (final Exception ignored) {
                    // that's fine, trunc data
                }

                break;

            case "int":
                try {
                    byteBuffer.putInt(Integer.parseInt(data));
                } catch (final Exception ignored) {
                    // that's fine, trunc data
                }

                break;

            case "long":
                try {
                    byteBuffer.putLong(Long.parseLong(data));
                } catch (final Exception ignored) {
                    // that's fine, trunc data
                }

                break;

            case "hex":
                String[] tokens = data.split(" ");

                for (String str : tokens) {
                    try {
                        byteBuffer.put((byte) Integer.parseInt(str, 16));
                    } catch (final Exception ignored) {
                        // that's fine, trunc data
                    }
                }

                break;

            default:
                p_stdout.printflnErr("Unsupported data type %s", type);
                return;
        }

        // put chunk back
        if (chunkService.put(chunk) != 1) {
            p_stdout.printflnErr("Putting chunk 0x%X failed: %s", cid, chunk.getState());
        } else {
            p_stdout.printfln("Put to chunk 0x%X successful", cid);
        }
    }

    @Override
    public List<String> getArgumentCompletionSuggestions(final int p_argumentPos, final TerminalCommandString p_cmdStr,
            final TerminalServiceAccessor p_services) {
        return Collections.emptyList();
    }

    private static boolean isType(final String p_str) {
        switch (p_str) {
            case "str":
            case "byte":
            case "short":
            case "int":
            case "long":
                return true;
            default:
                return false;
        }
    }
}
