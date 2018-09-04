package de.hhu.bsinfo.app;

import com.google.gson.annotations.Expose;

import java.util.ArrayList;

import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.app.AbstractApplication;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.chunk.ChunkRemoveService;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;
import de.hhu.bsinfo.app.dxramfspeer.*;
import de.hhu.bsinfo.app.dxramfscore.*;
import de.hhu.bsinfo.app.dxramfscore.rpc.*;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxram.data.ChunkID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import de.hhu.bsinfo.dxram.generated.BuildConfig;

public class DxramFsApp extends AbstractApplication {
    // +++++++ these constants are dummyies! you get the values from config!
    @Expose
    private int dxnet_local_id = 40;
    @Expose
    private String dxnet_local_addr = "127.0.0.1";
    @Expose
    private int dxnet_local_port = 6500;
    @Expose
    private int dxnet_local_peer_id = 41;
    @Expose
    private String dxnet_local_peer_addr = "127.0.0.1";
    @Expose
    private int dxnet_local_peer_port = 6501;
    @Expose
    private int dxnet_local_peer2_id = 42;
    @Expose
    private String dxnet_local_peer2_addr = "127.0.0.1";
    @Expose
    private int dxnet_local_peer2_port = 6502;
    @Expose
    private String ROOT_Chunk = "dummy";
    @Expose
    private int file_blocksize = 4*1024*1024;
    @Expose
    private int ref_ids_each_fsnode = 128;
    @Expose
    private int max_pathlength_chars = 512;
    @Expose
    private int max_filenamelength_chars = 128;
    @Expose
    private int max_hostlength_chars = 80;
    @Expose
    private int max_addrlength_chars = 48;

    // ++++++++ --------------------------
    
    private static final Logger LOG = LogManager.getFormatterLogger(DxramFsApp.class.getSimpleName());

    private long ROOT_CID;
    private BootService bootS;
    private ChunkService chunkS;
    private ChunkRemoveService removeS;
    private NameserviceService nameS;

    private FsNodeChunk ROOTN;
    private DxnetInit dxnetInit;
    
    public static NodePeerConfig nopeConfig;
    private NodePeerConfig.PeerConfig myNodePeerConfig;
    
    private boolean doEndlessLoop = true;


    @Override
    public void signalShutdown() {
        // no loops to interrupt or things to clean up ?
        doEndlessLoop = false;
        
        // dxnet or dxram shutdown??
    }
    
    @Override
    public DXRAMVersion getBuiltAgainstVersion() {
        return BuildConfig.DXRAM_VERSION;
    }

    @Override
    public String getApplicationName() {
        return "DxramFsApp";
    }

    @Override
    public boolean useConfigurationFile() {
        return true;
    }

    @Override
    public void main() {
        bootS = getService(BootService.class);
        chunkS = getService(ChunkService.class);
        nameS = getService(NameserviceService.class);
        removeS = getService(ChunkRemoveService.class);

        System.out.println(
                "application " + getApplicationName() + " on a peer" +
                        " and my dxram node id is " + NodeID.toHexString(bootS.getNodeID())
        );
        System.out.println("Where your Hadoop Node is:");
        System.out.println("DXNET dxnet_local_id  : " + dxnet_local_id);
        System.out.println("DXNET dxnet_local_addr: " + dxnet_local_addr);
        System.out.println("DXNET dxnet_local_port: " + dxnet_local_port);

        nopeConfig = new NodePeerConfig();
        // the single client hadoop dxnet peer, which sends rpc via dxnet
        nopeConfig.nodeId = (short) dxnet_local_id;
        nopeConfig.addr = dxnet_local_addr;
        nopeConfig.port = dxnet_local_port;

        nopeConfig.dxPeers = new ArrayList<>();

        // @todo if you realy want to run many dxram/dxnet peers on local, you have to change this !!
        System.out.println("How Hadoop DxramFs has to connect me:");
        System.out.println("DXNET dxnet_local_peer_id  : " + dxnet_local_peer_id);
        System.out.println("DXNET dxnet_local_peer_addr: " + dxnet_local_peer_addr);
        System.out.println("DXNET dxnet_local_peer_port: " + dxnet_local_peer_port);
        
        System.out.println("or for debugging, testing, developing:");
        System.out.println("DXNET dxnet_local_peer2_id  : " + dxnet_local_peer2_id);
        System.out.println("DXNET dxnet_local_peer2_addr: " + dxnet_local_peer2_addr);
        System.out.println("DXNET dxnet_local_peer2_port: " + dxnet_local_peer2_port);
        
        
        NodePeerConfig.PeerConfig aPeer = new NodePeerConfig.PeerConfig();
        aPeer.nodeId = (short) dxnet_local_peer_id;
        aPeer.addr = dxnet_local_peer_addr;
        aPeer.port = dxnet_local_peer_port;
        nopeConfig.dxPeers.add(aPeer);
        
        //for debug, testing, developing:
        NodePeerConfig.PeerConfig aPeer2 = new NodePeerConfig.PeerConfig();
        aPeer2.nodeId = (short) dxnet_local_peer2_id;
        aPeer2.addr = dxnet_local_peer2_addr;
        aPeer2.port = dxnet_local_peer2_port;
        nopeConfig.dxPeers.add(aPeer2);
 
        DxramFsConfig.file_blocksize = file_blocksize;
        DxramFsConfig.ref_ids_each_fsnode = ref_ids_each_fsnode;
        DxramFsConfig.max_pathlength_chars = max_pathlength_chars;
        DxramFsConfig.max_filenamelength_chars = max_filenamelength_chars;
        DxramFsConfig.max_hostlength_chars = max_hostlength_chars;
        DxramFsConfig.max_addrlength_chars = max_addrlength_chars;
        
        // local default: the first/only dxnet peer will be the rpc handling server
        myNodePeerConfig = aPeer;
        dxnetInit = new DxnetInit(nopeConfig, myNodePeerConfig.nodeId);
        
        ROOTN = new FsNodeChunk();
        if (nameS.getChunkID(ROOT_Chunk, 10) == ChunkID.INVALID_ID) {
            //for debug, testing, developing:
            //myNodePeerConfig = aPeer;
            //dxnetInit = new DxnetInit(nopeConfig, myNodePeerConfig.nodeId); // peer1
            
            // initial, if root does not exists
            ROOTN.get().init();
            ROOT_CID = chunkS.create(ROOTN.sizeofObject(), 1)[0];
            nameS.register(ROOT_CID, ROOT_Chunk);
            // maybe a new chunkid after register chunk with string in ROOT_Chunk
            ROOT_CID = nameS.getChunkID(ROOT_Chunk, 10);
            ROOTN.setID(ROOT_CID);
            chunkS.get(ROOTN);
            ROOTN.get().init();
            ROOTN.get().type = FsNodeType.FOLDER;
            ROOTN.get().name = "/";
            ROOTN.get().size = 0;
            ROOTN.get().refSize = 0;
            ROOTN.get().backId = ROOT_CID;
            ROOTN.get().forwardId = ROOT_CID;
            chunkS.put(ROOTN);
            LOG.debug("Create Root / on Chunk [%s]", String.format("0x%X", ROOTN.getID()));

        } else {
            //for debug, testing, developing:
            //myNodePeerConfig = aPeer2;
            //dxnetInit = new DxnetInit(nopeConfig, myNodePeerConfig.nodeId); // peer2

            ROOT_CID = nameS.getChunkID(ROOT_Chunk, 10);
            ROOTN.setID(ROOT_CID);
            chunkS.get(ROOTN);
        }
        
        while (doEndlessLoop) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {}
            
            // response is sent to the original sender of the message
            // -> maybe to handle requests from other hadoop nodes in the future

            if (dxnetInit.emh.gotResult()) {
                ExistsMessage msg = (ExistsMessage) dxnetInit.emh.Result();
                // @todo: geht nicht reuse() und isResponse() ? wie ist das angedacht?
                ExistsMessage response = externalHandleExists(msg);
                try {
                    dxnetInit.getDxNet().sendMessage(response);
                } catch (NetworkException e) {
                    e.printStackTrace();
                }
            }

            if (dxnetInit.idmh.gotResult()) {
                IsDirectoryMessage msg = (IsDirectoryMessage) dxnetInit.idmh.Result();
                IsDirectoryMessage response = externalHandleIsDirectory(msg);
                try {
                    dxnetInit.getDxNet().sendMessage(response);
                } catch (NetworkException e) {
                    e.printStackTrace();
                }
            }

            if (dxnetInit.flmh.gotResult()) {
                FileLengthMessage msg = (FileLengthMessage) dxnetInit.flmh.Result();
                FileLengthMessage response = externalHandleFileLength(msg);
                try {
                    dxnetInit.getDxNet().sendMessage(response);
                } catch (NetworkException e) {
                    e.printStackTrace();
                }
            }

            if (dxnetInit.mdmh.gotResult()) {
                MkDirsMessage msg = (MkDirsMessage) dxnetInit.mdmh.Result();
                MkDirsMessage response = externalHandleMkDirs(msg);
                try {
                    dxnetInit.getDxNet().sendMessage(response);
                } catch (NetworkException e) {
                    e.printStackTrace();
                }
            }
            
            if (dxnetInit.dmh.gotResult()) {
                DeleteMessage msg = (DeleteMessage) dxnetInit.dmh.Result();
                DeleteMessage response = externalHandleDelete(msg);
                try {
                    dxnetInit.getDxNet().sendMessage(response);
                } catch (NetworkException e) {
                    e.printStackTrace();
                }
            }
            
            if (dxnetInit.rtmh.gotResult()) {
                RenameToMessage msg = (RenameToMessage) dxnetInit.rtmh.Result();
                RenameToMessage response = externalHandleRenameTo(msg);
                try {
                    dxnetInit.getDxNet().sendMessage(response);
                } catch (NetworkException e) {
                    e.printStackTrace();
                }
            }

            // todo: andere Message Handler beifügen - ggf array oder so machen?

            //chunkS.get(ROOTN);
            //LOG.debug(String.valueOf(ROOTN.get().refSize));
            //ROOTN.get().refSize += myNodePeerConfig.nodeId;
            //chunkS.put(ROOTN);
        }
    }



    // ------------------------------------------------------------------------------------------------

    /**
     * Get the ChunkId of a FsNode with "name" in FsNode (a folder) or -1, if it not exists
     * @param name
     * @param nodeChunk
     * @return
     */
    private long getIn(String name, FsNodeChunk nodeChunk) {
        int size = nodeChunk.get().refSize;
        String nname = new String(name.getBytes(DxramFsConfig.STRING_STD_CHARSET));
        for (int i=0; i<size; i++) {
            if (i < DxramFsConfig.ref_ids_each_fsnode) {
                long entryChunkId = nodeChunk.get().refIds[i];
                FsNodeChunk entryChunk = new FsNodeChunk(entryChunkId);
                chunkS.get(entryChunk);
                LOG.debug("getIn: is '" + entryChunk.get().name + "' == '" + nname + "' ?");
                
                if (nname.equals(entryChunk.get().name)) {
                    return entryChunkId;
                }
            } else {
                // @todo an die forwardId rann gehen, wenn refSize = ref_ids_each_fsnode
            }
        }
        return ChunkID.INVALID_ID;
    }

    private String exists(String path) {
        String back = "OK";

        String[] pathparts = path.split("/");
        LOG.debug(String.join(" , ", pathparts));
        chunkS.get(ROOTN);
        if (path.length() == 0) {
            back = "OK / exists";
        } else if (ROOTN.get().size < 1) {
            back = "no / empty";
        } else {
            FsNodeChunk subNode = ROOTN;
            for (int i=0; i < pathparts.length; i++) {
                long subChunkId = getIn(pathparts[i], subNode);
                if (subChunkId == ChunkID.INVALID_ID) return "no";
                subNode = new FsNodeChunk(subChunkId);
                chunkS.get(subNode);
            }
            // for ended without return: thus the path+file must exists!
        }
        return back;
    }

    private String delete(String path) {
        String back = "OK";

        String[] pathparts = path.split("/");
        LOG.debug(String.join(" , ", pathparts));
        chunkS.get(ROOTN);
        if (ROOTN.get().size < 1) {
            return "OK / is still empty";
        } else {
            FsNodeChunk subNode = ROOTN;
            long subChunkId = ROOT_CID;
            long parentId = ROOT_CID;
            for (int i=0; i < pathparts.length; i++) {
                subChunkId = getIn(pathparts[i], subNode);
                if (subChunkId == ChunkID.INVALID_ID) return "no";
                subNode = new FsNodeChunk(subChunkId);
                chunkS.get(subNode);
                LOG.debug("Found " + subNode.get().name);
                parentId = subNode.get().backId;
            }
            if (!deleteThat(subNode)) {
                back = "no. fail on " + path;
            } else {
                subNode = new FsNodeChunk(parentId);
                chunkS.get(subNode);
                // @todo handle EXT
                if (subNode.get().size < ref_ids_each_fsnode) {
                    for (int j=0; j<subNode.get().refSize; j++) {
                        LOG.debug("refIds[%d] is Chunk [%s]", j, String.format("0x%X", subNode.get().refIds[j]));
                        if (subNode.get().refIds[j] == subChunkId) {
                            // resize refindex for new entry
                            subNode.get().refSize--;
                            // resize total size in the folder
                            subNode.get().size--;
                            // write last array entry to the position, we want to delete
                            subNode.get().refIds[j] = subNode.get().refIds[subNode.get().refSize];
                            // overwrite last array entry with INVALID to set it free
                            subNode.get().refIds[subNode.get().refSize] = ChunkID.INVALID_ID;
                            chunkS.put(subNode);
                            return "OK";
                        }
                    }
                    LOG.debug("no [%s] in parent Chunk [%s]", String.format("0x%X", subChunkId), String.format("0x%X", subNode.getID()));
                    back = "fail finding/removing " + path + " from refIds of parent node " + subNode.get().name;
                } else {
                    back = "we did not handle nodes with more than ref_ids_each_fsnode entryies in it";
                }
            }
        }
        return back;
    }
    
    private boolean deleteThat(FsNodeChunk nodeChunk) {
        long size = nodeChunk.get().size;
        long refSize = nodeChunk.get().refSize;
        LOG.debug("node size %d in %s", size, nodeChunk.get().name);
        
        if (nodeChunk.get().type == FsNodeType.FOLDER && size == 0) {
            // it is a empty folder, we can delete it
            
            // we are root? - we do not want to delete root here
            if (nodeChunk.getID() == ROOT_CID) return false;
            if (removeS.remove(nodeChunk.getID()) != 1) return false;
            return true;
        } else {
            // @todo delete FILE and handle folder and files in EXT fsNodes
        }
        return false;
    }

    private String isDir(String path) {
        String back = "OK";

        String[] pathparts = path.split("/");
        LOG.debug(String.join(" , ", pathparts));
        chunkS.get(ROOTN);
        if (path.length() == 0) {
            back = "OK / is a dir";
        } else if (ROOTN.get().refSize < 1) {
            back = "no / empty";
        } else {
            FsNodeChunk subNode = ROOTN;
            for (int i=0; i < pathparts.length; i++) {
                long subChunkId = getIn(pathparts[i], subNode);
                if (subChunkId == ChunkID.INVALID_ID) {
                    return "no it does not exist";
                }
                subNode = new FsNodeChunk(subChunkId);
                chunkS.get(subNode);
                // the complete patch must contains folders!
                if (subNode.get().type != FsNodeType.FOLDER) {
                    return "no it is not a folder";
                }
            }
        }
        return back;
    }

    private long mkDir(String name, FsNodeChunk parentNode) {
        FsNodeChunk newdir = new FsNodeChunk();
        newdir.get().init();
        //LOG.debug("before put " + name);
        //LOG.debug("config " + String.valueOf(DxramFsConfig.max_filenamelength_chars));
        chunkS.create(newdir);
        newdir.get().type = FsNodeType.FOLDER;
        newdir.get().name = new String(name.getBytes(DxramFsConfig.STRING_STD_CHARSET));
        newdir.get().backId = parentNode.getID();
        newdir.get().forwardId = newdir.getID();   // to self as dummy link
        newdir.get().size = 0;
        newdir.get().refSize = 0;
        chunkS.put(newdir);
        LOG.debug("Create %s on Chunk [%s]", name, String.format("0x%X", newdir.getID()));
        //LOG.debug("after put " + newdir.get().name);

        // @todo handle more than ref_ids_each_fsnode entries !!
        int refSize = parentNode.get().refSize;
        parentNode.get().refIds[refSize] = newdir.getID();
        parentNode.get().size++;
        parentNode.get().refSize++;

        // @todo error handling
        chunkS.put(parentNode);
        return newdir.getID();
    }

    // @todo: is this the correct behavior: /a/b exists and we want to move /c into /a/b but we did not get /a/b/c ! we get an error, that /a/b still exists!
    // ------> HINT: this error did not happend by using the hodoop dxramfs connector, because that code handles it!
    // ------> maybe the connector ask many "exists()" in that situation.
    // ------> for the future: test files rename!
    private String rename(String from, String to) {
        String back = "OK";
        if (from.length() == 0) return "fail. / not moveable.";
        if (exists(to).startsWith("OK")) return "fail. '"+to+"' still exists.";
        // @todo if "to" is a folder, we have to move a folder or file into that folder and check before, if this new structure exists,too!
        if (!exists(from).startsWith("OK")) return "fail. '"+from+"' does not exists.";

        String[] fromParts = from.split("/");
        String[] toParts = to.split("/");
        // get the from-chunk
        FsNodeChunk browseNode = ROOTN;
        long browseChunkId = ROOT_CID;
        chunkS.get(browseNode);
        for (int i=0; i < fromParts.length; i++) {
            browseChunkId = getIn(fromParts[i], browseNode);
            browseNode = new FsNodeChunk(browseChunkId);
            chunkS.get(browseNode);
        }
        FsNodeChunk fromChunk = browseNode;
        
        // get the parent-chunk of "from"
        FsNodeChunk oldParentChunk = new FsNodeChunk(fromChunk.get().backId);
        chunkS.get(oldParentChunk);
        
        // may create (and get) parent folder of "to"-file (or folder)
        browseNode = ROOTN;
        browseChunkId = ROOT_CID;
        chunkS.get(browseNode);
        for (int i = 0; i < toParts.length -1; i++) {
            // @todo: length -1 because we need the parent! but if "to" is a folder, we have to move it into the folder!
            browseChunkId = getIn(toParts[i], browseNode);
            if (browseChunkId == ChunkID.INVALID_ID) {
                browseChunkId = mkDir(toParts[i], browseNode);
            }
            browseNode = new FsNodeChunk(browseChunkId);
            chunkS.get(browseNode);
        }
        FsNodeChunk toParentChunk = browseNode;
        
        // rename the from-chunk
        // - handle move to root: @todo make a "move into a FOLDER" at this place!
        if (toParentChunk.getID() == ROOT_CID) {
            // name unchanged
            if (exists(fromParts[fromParts.length-1]).startsWith("OK")) return "no. something with that name still exists in /";
            fromChunk.get().backId = toParentChunk.getID();
        } else {
            fromChunk.get().name = new String(toParts[toParts.length-1].getBytes(DxramFsConfig.STRING_STD_CHARSET));
            fromChunk.get().backId = toParentChunk.getID();
        }
        // @todo error handling
        chunkS.put(fromChunk);
        
        // @todo handle more than ref_ids_each_fsnode entries !!!!!
        int refSize = toParentChunk.get().refSize;
        toParentChunk.get().refIds[refSize] = fromChunk.getID();
        toParentChunk.get().size++;
        toParentChunk.get().refSize++;

        // @todo error handling
        chunkS.put(toParentChunk);
        
        return back;
    }

    // ------------------------------------------------------------------------------------------------

    private ExistsMessage externalHandleExists(ExistsMessage msg) {
        ExistsMessage response = new ExistsMessage(msg.getSource(), exists(msg.get_data()));
        return response;
    }


    private MkDirsMessage externalHandleMkDirs(MkDirsMessage msg) {
        String back;
        String path = msg.get_data();
        String[] pathparts = path.split("/");
        LOG.debug(String.join(" , ", pathparts));

        if (path.length() > 0) {
            FsNodeChunk subNode = ROOTN;
            long subChunkId = ROOT_CID;
            for (int i = 0; i < pathparts.length; i++) {
                subChunkId = getIn(pathparts[i], subNode);
                if (subChunkId == ChunkID.INVALID_ID) {
                    // @todo: if we have to create the whole structure(?)
                    //return new MkDirsMessage((short) dxnet_local_id, "fail. upper folder not exists");
                    subChunkId = mkDir(pathparts[i], subNode);
                }
                subNode = new FsNodeChunk(subChunkId);
                chunkS.get(subNode);
            }

            // subNode should be the folder, where we have to create a new folder
            //mkDir(pathparts[pathparts.length -1], subNode);
            back = "OK " + String.valueOf(ROOTN.get().refSize);
        } else {
            back = "fail. path empty";
        }

        MkDirsMessage response = new MkDirsMessage(msg.getSource(), back);
        return response;
    }

    private IsDirectoryMessage externalHandleIsDirectory(IsDirectoryMessage msg) {
        IsDirectoryMessage response = new IsDirectoryMessage(msg.getSource(), isDir(msg.getData()));
        return response;
    }

    private FileLengthMessage externalHandleFileLength(FileLengthMessage msg) {
        String path = msg.get_data();
        long fileLength = -1;
        String back = "OK";

        String[] pathparts = path.split("/");
        LOG.debug(String.join(" , ", pathparts));
        chunkS.get(ROOTN);
        if (path.length() == 0) {
            back = "no / is a dir";
        } else if (ROOTN.get().size < 1) {
            back = "no / empty";
        } else {
            boolean failure = false;
            FsNodeChunk subNode = ROOTN;
            int i;
            long subChunkId;
            for (i=0; i < (pathparts.length -1); i++) {
                subChunkId = getIn(pathparts[i], subNode);
                if (subChunkId == ChunkID.INVALID_ID) {
                    back = "no: path part does not exist";
                    failure = true;
                    break;
                } else {
                    subNode = new FsNodeChunk(subChunkId);
                    chunkS.get(subNode);
                    if (subNode.get().type != FsNodeType.FOLDER) {
                        back = "no: path part is not a folder";
                        failure = true;
                        break;
                    }
                }
            }
            if (!failure) {
                // access to the last path part (i = pathparts.length -1)
                subChunkId = getIn(pathparts[i], subNode);
                if (subChunkId == ChunkID.INVALID_ID) {
                    back = "no: file does not exist";
                } else {
                    subNode = new FsNodeChunk(subChunkId);
                    chunkS.get(subNode);
                    if (subNode.get().type == FsNodeType.FILE) {
                        fileLength = subNode.get().size;
                    } else {
                        back = "no: it is a folder or EXT?!";
                    }
                }
            }
        }
        
        FileLengthMessage response = new FileLengthMessage(msg.getSource(), back);
        response.set_length(fileLength);
        return response;
    }
    
    
    // todo: delete must be atomic/sync to all hadoop nodes
    private DeleteMessage externalHandleDelete(DeleteMessage msg) {
        // recursion handles my connector code (listFiles...) in hadoop !
        DeleteMessage response = new DeleteMessage(msg.getSource(), delete(msg.getData()));
        return response;
    }
    
    private RenameToMessage externalHandleRenameTo(RenameToMessage msg) {
        RenameToMessage response = new RenameToMessage(msg.getSource(), rename(msg.getData(), msg.getToData()), "-");
        return response;
    }

}
