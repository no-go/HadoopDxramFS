package de.hhu.bsinfo.dxapp;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

import java.nio.charset.StandardCharsets;

public class HalloChunk extends AbstractChunk {

    private byte[] _data;
    private String _host;
    private int _port;
    
    public HalloChunk(int size) {
        super();
        _data = new byte[size];
        _host = "              x    ";
        _port = 60815;
    }

    public Hallo get() {
        Hallo dat = new Hallo();
        dat._data = _data;
        dat._host = _host;
        dat._port = _port;
        return dat;
    }
    
    public void set(Hallo dat) {
        _data = dat._data;
        _host = dat._host;
        _port = dat._port;
    }

    @Override
    public void importObject(final Importer p_importer) {
        p_importer.readBytes(_data);
        _host = p_importer.readString(_host);
        _port = p_importer.readInt(_port);
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeBytes(_data);
        p_exporter.writeString(_host);
        p_exporter.writeInt(_port);
    }

    @Override
    public int sizeofObject() {
        int size = 0;
        size += _data.length;
        size += ObjectSizeUtil.sizeofString(_host);
        size += Integer.BYTES;
        return size;
    }
    
    
    @Override
    public String toString() {
        return "HalloChunkBuffer[" + 
        ChunkID.toHexString(getID()) + 
        ", " + getState() + 
        ", '" + (new String(_data, StandardCharsets.US_ASCII)) + "'" +
        ", " + _host + 
        ", " + Integer.toString(_port) + 
        ']';
    }
    
}