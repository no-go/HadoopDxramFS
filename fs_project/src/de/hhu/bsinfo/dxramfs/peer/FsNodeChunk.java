package de.hhu.bsinfo.dxramfs.peer;

import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxramfs.core.FsNode;
import de.hhu.bsinfo.dxramfs.core.FsNodeType;
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
        p_exporter.writeLong(_fsNode.referenceId);
        p_exporter.writeString(_fsNode.name);
        p_exporter.writeInt(_fsNode.fsNodeType);
        p_exporter.writeLong(_fsNode.size);
        p_exporter.writeInt(_fsNode.entriesSize);
        p_exporter.writeLongArray(_fsNode.blockinfoIds);
        p_exporter.writeLong(_fsNode.extID);
    }

    @Override
    public void importObject(final Importer p_importer) {
        _fsNode.ID = p_importer.readLong(_fsNode.ID);
        _fsNode.referenceId = p_importer.readLong(_fsNode.referenceId);
        _fsNode.name = p_importer.readString(_fsNode.name);
        _fsNode.size = p_importer.readLong(_fsNode.size);
        _fsNode.fsNodeType = p_importer.readInt(_fsNode.fsNodeType);
        _fsNode.entriesSize = p_importer.readInt(_fsNode.entriesSize);
        _fsNode.blockinfoIds = p_importer.readLongArray(_fsNode.blockinfoIds);
        _fsNode.extID = p_importer.readLong(_fsNode.extID);
    }

    @Override
    public int sizeofObject() {
        int size = 0;
        size += Long.BYTES; // ID
        size += Long.BYTES; // referenceId
        size += _fsNode.name.getBytes().length; // name
        size += Long.BYTES; // size
        size += Integer.BYTES; // fsNodeType
        size += Integer.BYTES; // entriesSize
        size += ObjectSizeUtil.sizeofLongArray(_fsNode.blockinfoIds);
        size += Long.BYTES; // extID
        return size;
    }
}
