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
    public int fsNodeType;
    public long size; // file: total bytes
    public int entriesSize; // total number of blocks (or entries in this folder)

    // if we are a folder, we interpret this ids as chunkids to other FSNODES (and not chunkids to blockinfos) !!!!!
    public long[] blockinfoIds = new long[0];

    // refernce to a ext fsNodeChunk, if we need more than "blockinfo_ids_each_fsnode" blocks (and block infos)

    /*
    example: a directory /goo/ with 203 entries:
        ID: 4711
        name: "goo"
        fsNodeType: FOLDER
        referenceId: ROOT_CID -> "/"
        entriesSize: 203
        -> blockinfo_ids_each_fsnode = 100
        -> 203 >= 100  -> we have to use extID !
        extID: 4712

        ID: 4712
        name: "goo"
        fsNodeType: EXT
        referenceId: 4711
        entriesSize: 100 (it is full)
        -> 100 >= 100 -> we have to use extID !
        extID: 4713

        ID: 4713
        name: "goo"
        fsNodeType: EXT
        referenceId: 4712
        entriesSize: 3
        -> 3 < 100 -> we have space for new entries
        extID: -

    @todo Delete files in such an array +linked list gets ugly. we have to overwrite them with entries from the end.

     */
    public long extID;

    public FsNode() { }

    public void init() {
        this.blockinfoIds = new long[DxramFsConfig.blockinfo_ids_each_fsnode];
    }
}
