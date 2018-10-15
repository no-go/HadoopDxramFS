package de.hhu.bsinfo.app.dxramfspeer;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.app.dxramfscore.FsNode;
import de.hhu.bsinfo.app.dxramfscore.FsNodeType;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class FsNodeChunk extends AbstractChunk {
    private FsNode _fsNode;

    public FsNodeChunk() {
        _fsNode = new FsNode();
        //_fsNode.init(); //xx
    }

    public FsNodeChunk(final long p_id) {
        super(p_id);
        _fsNode = new FsNode();
        //_fsNode.init(); //xx
        setID(p_id);
    }

    @Override
    public void setID(final long p_id) {
        super.setID(p_id);
        _fsNode.ID = p_id;
    }

    public FsNode get() {
        return _fsNode;
    }

    // -----------------------------------------------------------------------------

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeLong(_fsNode.ID);
        p_exporter.writeLong(_fsNode.backId);
        p_exporter.writeString(_fsNode.name);
        p_exporter.writeInt(_fsNode.type);
        p_exporter.writeLong(_fsNode.size);
        p_exporter.writeInt(_fsNode.refSize);
        p_exporter.writeLongArray(_fsNode.refIds);
        p_exporter.writeLong(_fsNode.forwardId);
    }

    @Override
    public void importObject(final Importer p_importer) {
        _fsNode.ID = p_importer.readLong(_fsNode.ID);
        _fsNode.backId = p_importer.readLong(_fsNode.backId);
        _fsNode.name = p_importer.readString(_fsNode.name);
        _fsNode.type = p_importer.readInt(_fsNode.type);
        _fsNode.size = p_importer.readLong(_fsNode.size);
        _fsNode.refSize = p_importer.readInt(_fsNode.refSize);
        _fsNode.refIds = p_importer.readLongArray(_fsNode.refIds);
        _fsNode.forwardId = p_importer.readLong(_fsNode.forwardId);
    }

    @Override
    public int sizeofObject() {
        int size = 0;
        size += Long.BYTES; // ID
        size += Long.BYTES; // referenceId backId
        size += ObjectSizeUtil.sizeofString(_fsNode.name);
        //size += _fsNode.name.getBytes().length +1; // name @todo the +1 is a bit suspect
        size += Integer.BYTES; // fsNodeType type
        size += Long.BYTES; // size
        size += Integer.BYTES; // refSize
        size += ObjectSizeUtil.sizeofLongArray(_fsNode.refIds);
//        size += Long.BYTES * DxramFsConfig.ref_ids_each_fsnode;
        size += Long.BYTES; // extID forwardId
        return size; // it should be 1064 + sizeof string ascii encoded (""=1) + CompactNumber.getSizeOfNumber(_fsNode.refIds.length)
    }
};
