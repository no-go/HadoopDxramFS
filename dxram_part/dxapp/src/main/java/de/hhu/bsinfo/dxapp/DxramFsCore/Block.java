package de.hhu.bsinfo.dxapp.dxramfscore;

public class Block {

    /** _id is a BlockId and this is a ChunkId in dxram */
    public long ID;
    public byte[] _data = new byte[0];

    public Block() {}

    public void init() {
        this._data = new byte[DxramFsConfig.file_blocksize];
    }
};
