package de.hhu.bsinfo.dxramfs.core;

public class FsNode {
    // chunkid
    public long ID;
    // in a file: the chunkid of the folder
    // in a folder: the chunkid of parent folder
    // in a ext: the chunkid of parent ext or file
    public long referenceId;
    // the file or folder name (without / in it - only root has a single / as name)
    public String name;
    // file, folder or ext (stores more blockinfoIds)
    public FsNodeType fsNodeType;
    public long size; // file: total bytes
    public long entriesSize; // total number of blocks (or entries in this folder)

    // if we are a folder, we interpret this ids as other FSNODES !!!!!
    public long[] blockinfoIds = new long[0];
    // refernce to a ext fsNodeChunk, if we need more than blocks (and block infos)
    public long extID;

    public FsNode() { }

    public void init() {
        this.blockinfoIds = new long[DxramFsConfig.blockinfo_ids_each_fsnode];
    }
}
