package de.hhu.bsinfo.dxramfs.core;

public class BlockChunk {

    /** _id is a BlockId and this is a ChunkId in dxram */
    public long ID;
    private byte[] _data;

    public BlockChunk() {
    }

    public byte[] getData() {
        return _data;
    }

    public void setData(byte[] data) {
        _data = data;
    }

    public void setData(int idx, byte data) {
        _data[idx] = data;
    }

    public void initData() {
        this._data = new byte[DxramFsConfig.file_blocksize];
    }
}
