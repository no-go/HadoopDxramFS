package de.hhu.bsinfo.app.dxramfspeer;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.app.dxramfscore.Block;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class BlockChunk extends AbstractChunk {
    private Block _block;

    public BlockChunk() {
        _block = new Block();
    }

    public BlockChunk(final long p_id) {
        super(p_id);
        _block = new Block();
        setID(p_id);
    }

    @Override
    public void setID(final long p_id) {
        super.setID(p_id);
         _block.ID = p_id;
    }

    public Block get() {
        return _block;
    }

    // -----------------------------------------------------------------------------

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeLong(_block.ID);
        p_exporter.writeByteArray(_block._data);
    }

    @Override
    public void importObject(final Importer p_importer) {
        _block.ID = p_importer.readLong(_block.ID);
        _block._data = p_importer.readByteArray(_block._data);
    }

    @Override
    public int sizeofObject() {
        int size = 0;
        size += Long.BYTES; // _block.ID
        size += ObjectSizeUtil.sizeofByteArray(_block._data);
        return size;
    }
};
