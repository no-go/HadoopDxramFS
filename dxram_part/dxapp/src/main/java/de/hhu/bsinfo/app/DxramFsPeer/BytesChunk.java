package de.hhu.bsinfo.app.dxramfspeer;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

import java.nio.charset.StandardCharsets;

public class BytesChunk extends AbstractChunk {
//public final class BytesChunk extends AbstractChunk {

    private Door _door;
    
    public BytesChunk(final int p_bufferSize) {
        super();
        _door = new Door();
        _door._data = new byte[p_bufferSize];
        //_door._addr = "dummy";
        _door._port = 60815;
    }

    public BytesChunk(final long p_id, final int p_bufferSize) {
        super(p_id);
        _door = new Door();
        _door._data = new byte[p_bufferSize];
        //_door._addr = "dummy";
        _door._port = 60815;
    }

    public BytesChunk(final byte[] p_buffer) {
        super();
        _door = new Door();
        _door._data = p_buffer;
        //_door._addr = "dummy";
        _door._port = 60815;
    }

    public BytesChunk(final long p_id, final byte[] p_buffer) {
        super(p_id);
        _door = new Door();
        _door._data = p_buffer;
        //_door._addr = "dummy";
        _door._port = 60815;
    }

    public Door get() {
        return _door;
    }
    
    public void set(Door dat) {
        _door = dat;
    }

    @Override
    public void importObject(final Importer p_importer) {
        p_importer.readBytes(_door._data);
        //p_importer.readString(_door._addr);
        p_importer.readInt(_door._port);
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeBytes(_door._data);
       // p_exporter.writeString(_door._addr);
        p_exporter.writeInt(_door._port);
    }

    @Override
    public int sizeofObject() {
        int size = 0;
        size += ObjectSizeUtil.sizeofByteArray(_door._data);
        //size += ObjectSizeUtil.sizeofString(_door._addr);
        size += Integer.BYTES;
        return size;
    }
    
    @Override
    public String toString() {
        //return "BytesChunkBuffer[" + ChunkID.toHexString(getID()) + ", " + getState() + ", " + _door._addr + ']';
        //return "BytesChunkBuffer[" + ChunkID.toHexString(getID()) + ", " + getState() + ", " + (new String(_door._data, StandardCharsets.US_ASCII)) + ']';
        return "BytesChunkBuffer[" + ChunkID.toHexString(getID()) + ", " + getState() + ", " + Integer.toString(_door._data.length) + ']';
    }
}