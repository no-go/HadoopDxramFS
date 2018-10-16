package de.hhu.bsinfo.dxapp;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

import java.nio.charset.StandardCharsets;

public class HalloChunk extends AbstractChunk {

    private Hallo _hallo;
    
    public HalloChunk(final int p_bufferSize) {
        super();
        _hallo = new Hallo();
        _hallo._data = new byte[p_bufferSize];
        _hallo._port = 60815;
    }

    public HalloChunk(final long p_id, final int p_bufferSize) {
        super(p_id);
        _hallo = new Hallo();
        _hallo._data = new byte[p_bufferSize];
        _hallo._port = 60815;
    }

    public HalloChunk(final byte[] p_buffer) {
        super();
        _hallo = new Hallo();
        _hallo._data = p_buffer;
        _hallo._port = 60815;
    }

    public HalloChunk(final long p_id, final byte[] p_buffer) {
        super(p_id);
        _hallo = new Hallo();
        _hallo._data = p_buffer;
        _hallo._port = 60815;
    }

    public Hallo get() {
        return _hallo;
    }
    
    public void set(Hallo dat) {
        _hallo = dat;
    }

    @Override
    public void importObject(final Importer p_importer) {
        p_importer.readBytes(_hallo._data);
        p_importer.readInt(_hallo._port);
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeBytes(_hallo._data);
        p_exporter.writeInt(_hallo._port);
    }

    @Override
    public int sizeofObject() {
        int size = 0;
        size += ObjectSizeUtil.sizeofByteArray(_hallo._data);
        size += Integer.BYTES;
        return size;
    }
    
    @Override
    public String toString() {
        return "BytesChunkBuffer[" + ChunkID.toHexString(getID()) + ", " + getState() + ", " + (new String(_hallo._data, StandardCharsets.US_ASCII)) + ']';
        //return "BytesChunkBuffer[" + ChunkID.toHexString(getID()) + ", " + getState() + ", " + Integer.toString(_hallo._data.length) + ']';
    }
}