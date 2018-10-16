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
    }

    /*
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
    */

    public Hallo get() {
        return _hallo;
    }
    
    public void set(Hallo dat) {
        _hallo = dat; // flat copy?
        //_hallo._data = dat._data;
        //_hallo._host = dat._host;
        //_hallo._port = dat._port;
    }

    @Override
    public void importObject(final Importer p_importer) {
        p_importer.readBytes(_hallo._data);
        p_importer.readString(_hallo._host);
        p_importer.readInt(_hallo._port);
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeBytes(_hallo._data);
        p_exporter.writeString(_hallo._host);
        p_exporter.writeInt(_hallo._port);
    }

    @Override
    public int sizeofObject() {
        int size = 0;
        size += _hallo._data.length;
        size += ObjectSizeUtil.sizeofString(_hallo._host);
        size += Integer.BYTES;
        return size;
    }
    
    
    @Override
    public String toString() {
        return "HalloChunkBuffer[" + 
        ChunkID.toHexString(getID()) + 
        ", " + getState() + 
        ", '" + (new String(_hallo._data, StandardCharsets.US_ASCII)) + "'" +
        ", " + _hallo._host + 
        ", " + Integer.toString(_hallo._port) + 
        ']';
    }
    
}