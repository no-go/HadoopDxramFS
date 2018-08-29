package de.hhu.bsinfo.app.dxramfspeer;

import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.app.dxramfscore.Blockinfo;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class BlockinfoChunk extends DataStructure {
    private Blockinfo _blockinfo;

    public BlockinfoChunk() {
        _blockinfo = new Blockinfo();
    }

    public BlockinfoChunk(final long p_id) {
        super(p_id);
        _blockinfo = new Blockinfo();
        setID(p_id);
    }

    @Override
    public void setID(final long p_id) {
        super.setID(p_id);
        _blockinfo.ID = p_id;
    }

    public Blockinfo get() {
        return _blockinfo;
    }

    // -----------------------------------------------------------------------------

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeLong(_blockinfo.ID);
        p_exporter.writeLong(_blockinfo.offset);
        p_exporter.writeLong(_blockinfo.length);
        p_exporter.writeBoolean(_blockinfo.corrupt);
        p_exporter.writeLong(_blockinfo.storageId);
        p_exporter.writeString(_blockinfo.host);
        p_exporter.writeString(_blockinfo.addr);
        p_exporter.writeInt(_blockinfo.port);
    }

    @Override
    public void importObject(final Importer p_importer) {
        _blockinfo.ID = p_importer.readLong(_blockinfo.ID);
        _blockinfo.offset = p_importer.readLong(_blockinfo.offset);
        _blockinfo.length = p_importer.readLong(_blockinfo.length);
        _blockinfo.corrupt = p_importer.readBoolean(_blockinfo.corrupt);
        _blockinfo.storageId = p_importer.readLong(_blockinfo.storageId);
        _blockinfo.host = p_importer.readString(_blockinfo.host);
        _blockinfo.addr = p_importer.readString(_blockinfo.addr);
        _blockinfo.port = p_importer.readInt(_blockinfo.port);
    }

    @Override
    public int sizeofObject() {
        int size = 0;
        size += Long.BYTES; // ID
        size += Long.BYTES; // offset
        size += Long.BYTES; // length
        /// @todo I use a complete byte to store a bit? yes, this is what serilisation do here
        size += Byte.BYTES; // corrupt
        size += Long.BYTES; // storageId
        size += ObjectSizeUtil.sizeofString(_blockinfo.host);
        size += ObjectSizeUtil.sizeofString(_blockinfo.addr);
        size += Integer.BYTES; // port
        return size;
    }
};
