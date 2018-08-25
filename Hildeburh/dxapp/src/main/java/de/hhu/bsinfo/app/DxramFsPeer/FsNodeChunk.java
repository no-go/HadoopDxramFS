package de.hhu.bsinfo.app.dxramfspeer;

import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.app.dxramfscore.DxramFsConfig;
import de.hhu.bsinfo.app.dxramfscore.FsNode;
import de.hhu.bsinfo.app.dxramfscore.FsNodeType;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class FsNodeChunk extends DataStructure {
    private FsNode _fsNode;

    public FsNodeChunk() {
        _fsNode = new FsNode();
        _fsNode.init();
    }

    public FsNodeChunk(final long p_id) {
        super(p_id);
        _fsNode = new FsNode();
        _fsNode.init();
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
        _fsNode.size = p_importer.readLong(_fsNode.size);
        _fsNode.type = p_importer.readInt(_fsNode.type);
        _fsNode.refSize = p_importer.readInt(_fsNode.refSize);
        _fsNode.refIds = p_importer.readLongArray(_fsNode.refIds);
        _fsNode.forwardId = p_importer.readLong(_fsNode.forwardId);
    }

    @Override
    public int sizeofObject() {
        int size = 0;
        size += Long.BYTES; // ID
        size += Long.BYTES; // referenceId backId
        size += _fsNode.name.getBytes().length; // name
        size += Long.BYTES; // size
        size += Integer.BYTES; // fsNodeType type
        size += Integer.BYTES; // entriesSize
        size += ObjectSizeUtil.sizeofLongArray(_fsNode.refIds);
//        size += Long.BYTES * DxramFsConfig.ref_ids_each_fsnode;
        size += Long.BYTES; // extID forwardId
        return size;
    }
};
