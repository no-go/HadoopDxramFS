package de.hhu.bsinfo.dxramfs.core;

public class FsNodeChunk {
    // chunkid
    public long ID;
    // in a file: the chunkid of the folder
    // in a folder: the chunkid of parent folder
    // in a ext: the chunkid of parent ext or file
    public long referenceId;
    // the file or folder name (without / in it)
    public String name;
    // file, folder or ext (stores more blockinfoIds)
    public FsNodeType fsNodeType;
    public long size; // file: bytes, folder: nomber of files/folders of that folder
    public long entriesSize; // number of blocks
    public long blockinfoIds[];
    // refernce to a ext fsNodeChunk, if we need more than blocks (and block infos)
    public long extID;

    public FsNodeChunk() {
        blockinfoIds = new long[DxramFsConfig.blockinfo_ids_each_fsnode];
    }
}
