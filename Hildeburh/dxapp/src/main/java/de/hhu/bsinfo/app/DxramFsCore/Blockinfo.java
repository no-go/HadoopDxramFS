package de.hhu.bsinfo.app.dxramfscore;


public class Blockinfo {
    public long ID; // chunkid of that BlockinfoChunk
    public long offset;  // @todo Offset of the block in the file (do I need this? Did I need it for append?)
    public long length; // @todo home many byte did we need from this block?
    public boolean corrupt; // @todo how does this happend?
    public long storageId; // to the BlockChunk id, where the data exists (only 1 id because no replica)
    /* we do not have replica like in the blocklocation class of hadoop */
    public String host;
    public String addr;
    public int port;
};

/*from BlockLocation

private String[] hosts; // Datanode hostnames
private String[] cachedHosts; // Datanode hostnames with a cached replica
private String[] names; // Datanode IP:xferPort for accessing the block
private String[] topologyPaths; // Full path name in network topology
private String[] storageIds; // Storage ID of each replica

private StorageType[] storageTypes; // Storage type of each replica -- we use RAM only
private long offset;  // Offset of the block in the file
private long length;
private boolean corrupt;

x id
x host muss via chunkid suche mit der storageid gesucht werden
x offset (von was ?)
x Länge
x corrupt
- keine replikat nummer (= storageId) weil immer 1
x link zu BlockinfoChunk des Replikats - nur 1 replikat (also keine replikation)
- KEINE Topologie info
- Speichertyp (sollte immer RAM sein, oder? Daher weglassen?)

*/