package de.hhu.bsinfo.dxapp.dxramfspeer;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxapp.dxramfscore.DxramFsConfig;
import de.hhu.bsinfo.dxapp.dxramfscore.DxramFsUtil;
import de.hhu.bsinfo.dxapp.dxramfscore.FsNode;
import de.hhu.bsinfo.dxapp.dxramfscore.FsNodeType;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class FsNodeChunk extends AbstractChunk {
    private long _myId;
    private long _backId;
    private byte[] _name;
    private int _nameSize;
    private int _type;
    private long _size;
    private int _refSize;
    private long[] _refIds;
    private long _forwardId;


    public FsNodeChunk() {
        _name = new byte[DxramFsConfig.max_filenamelength_chars];
        _refIds = new long[DxramFsConfig.ref_ids_each_fsnode];
    }
    
    public FsNodeChunk(final long p_id) {
        super(p_id);
        _myId = p_id;
        _name = new byte[DxramFsConfig.max_filenamelength_chars];
        _refIds = new long[DxramFsConfig.ref_ids_each_fsnode];
    }
    
    @Override
    public void setID(final long p_id) {
        super.setID(p_id);
        _myId = p_id;
    }

    public FsNode get() {
        FsNode f = new FsNode();
        f.init();
        f.ID = _myId;
        f.backId = _backId;
        f.name = DxramFsUtil.by2s(_name, _nameSize);
        f.type = _type;
        f.size = _size;
        f.refSize = _refSize;
        f.refIds = _refIds;
        f.forwardId = _forwardId;
        return f;
    }

    public void set(FsNode f) {
        setID(f.ID);
        _myId = f.ID;
        _backId = f.backId;
        _nameSize = DxramFsUtil.s2by(_name, f.name);
        _type = f.type;
        _size = f.size;
        _refSize = f.refSize;
        _refIds = f.refIds;
        _forwardId = f.forwardId;
    }

    // -----------------------------------------------------------------------------

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeLong(_myId);
        p_exporter.writeLong(_backId);
        p_exporter.writeByteArray(_name);
        p_exporter.writeInt(_nameSize);
        p_exporter.writeInt(_type);
        p_exporter.writeLong(_size);
        p_exporter.writeInt(_refSize);
        p_exporter.writeLongArray(_refIds);
        p_exporter.writeLong(_forwardId);
    }

    @Override
    public void importObject(final Importer p_importer) {
        _myId = p_importer.readLong(_myId);
        _backId = p_importer.readLong(_backId);
        p_importer.readByteArray(_name);
        _nameSize = p_importer.readInt(_nameSize);
        _type = p_importer.readInt(_type);
        _size = p_importer.readLong(_size);
        _refSize = p_importer.readInt(_refSize);
        _refIds = p_importer.readLongArray(_refIds);
        _forwardId = p_importer.readLong(_forwardId);
    }

    @Override
    public int sizeofObject() {
        int size = 0;
        size += Long.BYTES; // ID
        size += Long.BYTES; // referenceId or backId
        size += _name.length;
        size += Integer.BYTES; // _nameSize
        size += Integer.BYTES; // fsNodeType type
        size += Long.BYTES; // size
        size += Integer.BYTES; // refSize
        size += Long.BYTES * DxramFsConfig.ref_ids_each_fsnode;
        size += Long.BYTES; // extID forwardId
        return size;
    }
};
