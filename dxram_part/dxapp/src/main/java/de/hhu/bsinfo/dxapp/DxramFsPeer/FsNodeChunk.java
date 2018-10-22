package de.hhu.bsinfo.dxapp.dxramfspeer;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxapp.dxramfscore.DxramFsConfig;
import de.hhu.bsinfo.dxapp.dxramfscore.DxramFsUtil;
import de.hhu.bsinfo.dxapp.dxramfscore.FsNode;
import de.hhu.bsinfo.dxapp.dxramfscore.FsNodeType;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class FsNodeChunk extends AbstractChunk {

    private FsNode _fsNode;

    public FsNodeChunk() {
        _fsNode = new FsNode();
    }

    public FsNodeChunk(final long p_id) {
        super(p_id);
        _fsNode = new FsNode();
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
    
    public void set(FsNode f) {
        setID(f.ID);
        _fsNode = f;
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
        return size;
    }

    @Override
    public String toString() {
        return "Chunk[" + ChunkID.toHexString(getID()) + ", " + getState() + ", " + get().toString() + "]";
    }

    /* ------------------------vvv this commented code crashes on 2.nd peer, if it wants the root FsNodeChunk
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
    */
}
