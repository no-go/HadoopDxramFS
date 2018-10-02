package de.hhu.bsinfo.app;

import com.google.gson.annotations.Expose;

import java.util.ArrayList;
import java.net.InetSocketAddress;

import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.app.AbstractApplication;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.lookup.LookupService;
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
    // +++++++ these constants are dummies! you get the values from config!
    @Expose
    private String dxnet_to_dxram_peers = "id@ip:dxnetport@,dxnet node id @ ip:dxnetport@ip:dxRAMport,...";
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
    private LookupService lookS;
    private ChunkRemoveService removeS;
    private NameserviceService nameS;

    private FsNodeChunk ROOTN;
    private DxnetInit dxnetInit;
    
    public static NodePeerConfig nopeConfig;
    
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
        lookS = getService(LookupService.class);
        chunkS = getService(ChunkService.class);
        nameS = getService(NameserviceService.class);
        removeS = getService(ChunkRemoveService.class);
        
        System.out.println(
                "application " + getApplicationName() + " on a peer" +
                        " and my dxram node id is " + NodeID.toHexString(bootS.getNodeID())
        );

        DxramFsConfig.file_blocksize = file_blocksize;
        DxramFsConfig.ref_ids_each_fsnode = ref_ids_each_fsnode;
        DxramFsConfig.max_pathlength_chars = max_pathlength_chars;
        DxramFsConfig.max_filenamelength_chars = max_filenamelength_chars;
        DxramFsConfig.max_hostlength_chars = max_hostlength_chars;
        DxramFsConfig.max_addrlength_chars = max_addrlength_chars;

        // if my dxram peer port and ip is ... I have to search a mapped dxnet ip and port from the config!
        // we submit it to the NodePeerConfig factory to match it

        short dxramNodeMeId = bootS.getNodeID();
        InetSocketAddress nodeDetail = bootS.getNodeAddress(dxramNodeMeId);

        nopeConfig = NodePeerConfig.factory(
            nodeDetail.getHostString(),
            nodeDetail.getAddress().getHostAddress(),
            nodeDetail.getPort(),
            dxnet_to_dxram_peers.split(",")
        );

        System.out.println("How Hadoop DxramFs can contact me:");
        System.out.println("DXNET dxnet peer_id  : " + nopeConfig.nodeId);
        System.out.println("DXNET dxnet peer_addr: " + nopeConfig.dxnet_addr);
        System.out.println("DXNET dxnet peer_port: " + nopeConfig.dxnet_port);
        
        dxnetInit = new DxnetInit(nopeConfig, nopeConfig.nodeId);
        
        
        ROOTN = new FsNodeChunk();
        if (nameS.getChunkID(ROOT_Chunk, 10) == ChunkID.INVALID_ID) {
            
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

            if (dxnetInit.lmh.gotResult()) {
                ListMessage msg = (ListMessage) dxnetInit.lmh.Result();
                ListMessage response = externalHandleList(msg);
                try {
                    dxnetInit.getDxNet().sendMessage(response);
                } catch (NetworkException e) {
                    e.printStackTrace();
                }
            }
            
            if (dxnetInit.askBlockmh.askMsg != null) {
                long askId = dxnetInit.askBlockmh.askMsg.getAskBlockId();
                short dest = dxnetInit.askBlockmh.askMsg.getSource();
                dxnetInit.askBlockmh.askMsg = null;
                BlockChunk blockChunk = new BlockChunk(askId);
                chunkS.get(blockChunk);
                GetBlockMessage response = null;
                if (blockChunk.getID() == ChunkID.INVALID_ID) {
                    response = new GetBlockMessage(dest);
                    response.setSuccess(false);
                } else {
                    response = new GetBlockMessage(dest, blockChunk.get());
                    response.setSuccess(true);
                }
                try {
                    dxnetInit.getDxNet().sendMessage(response);
                } catch (NetworkException e) {
                    e.printStackTrace();
                }
            }

            /* only hadoop got such messages
             * 
            if (dxnetInit.getBlockmh.gotResult()) {
                GetBlockMessage msg = (GetBlockMessage) dxnetInit.getBlockmh.Result();
                AskBlockMessage.success = msg.getSuccess();
                AskBlockMessage._result = msg.getData();
            }
            */

            if (dxnetInit.cmh.gotResult()) {
                CreateMessage msg = (CreateMessage) dxnetInit.cmh.Result();
                CreateMessage response = externalHandleCreate(msg);
                try {
                    dxnetInit.getDxNet().sendMessage(response);
                } catch (NetworkException e) {
                    e.printStackTrace();
                }
            }

            if (dxnetInit.fsnodemh.gotResult()) {
                FsNodeMessage msg = (FsNodeMessage) dxnetInit.fsnodemh.Result();
                FsNodeMessage response = externalHandleFsNode(msg);
                try {
                    dxnetInit.getDxNet().sendMessage(response);
                } catch (NetworkException e) {
                    e.printStackTrace();
                }
            }

            if (dxnetInit.fsnodeByIdmh.gotResult()) {
                FsNodeByIdMessage msg = (FsNodeByIdMessage) dxnetInit.fsnodeByIdmh.Result();
                FsNodeByIdMessage response = externalHandleFsNodeById(msg);
                try {
                    dxnetInit.getDxNet().sendMessage(response);
                } catch (NetworkException e) {
                    e.printStackTrace();
                }
            }
            
            if (dxnetInit.bimh.gotResult()) {
                BlockinfoMessage msg = (BlockinfoMessage) dxnetInit.bimh.Result();
                BlockinfoMessage response = externalHandleBlockinfo(msg);
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
    // handles EXT
    private long getIn(String name, FsNodeChunk nodeChunk) {
        // nodeChunk is a folder and we do not believe it needs "long" to count the files in it!
        int size = (int) nodeChunk.get().size; 
        String nname = new String(name.getBytes(DxramFsConfig.STRING_STD_CHARSET));
        int charCount = 0;
        int extCount = 0;
        int indexInExt = 0;
        FsNodeChunk getRefsIn = nodeChunk;
        
        // handle some specials about the root entry
        if (name.equals("") || name.equals("/")) {
            return ROOT_CID;
        }
        
        for (int i=0; i<size; i++) {
            indexInExt = i - extCount * DxramFsConfig.ref_ids_each_fsnode;
            if (indexInExt < DxramFsConfig.ref_ids_each_fsnode) {
                long entryChunkId = getRefsIn.get().refIds[indexInExt];
                FsNodeChunk entryChunk = new FsNodeChunk(entryChunkId);
                chunkS.get(entryChunk);
                LOG.debug("getIn: is '" + entryChunk.get().name + "' == '" + nname + "' ?");
                
                if (nname.equals(entryChunk.get().name)) {
                    return entryChunkId;
                }
            } else {
                getRefsIn = new FsNodeChunk(getRefsIn.get().forwardId);
                chunkS.get(getRefsIn);
                LOG.debug("getIn: Need Chunk [%s] as EXT FsNode", String.format("0x%X", getRefsIn.getID()));
                extCount++;
                i--;
            }
        }
        return ChunkID.INVALID_ID;
    }

    // handles EXT
    private String[] list(String name, int startidx) {
        return list(name, startidx, -1);
    }
    
    // handles EXT
    private String[] list(String path, int startidx, int maxJoinChars) {
        String[] pathparts = path.split("/");
        FsNodeChunk subNode = ROOTN;
        long subChunkId = ROOT_CID;
        chunkS.get(subNode);
        if (subNode.get().size < 1) { // FS is empty
            return null;
        } else {
            for (int i=0; i < pathparts.length; i++) {
                subChunkId = getIn(pathparts[i], subNode);
                if (subChunkId == ChunkID.INVALID_ID) {
                    // @todo how to handle "not exists" ?!
                    return null;
                }
                subNode = new FsNodeChunk(subChunkId);
                chunkS.get(subNode);
            }
            return list(subNode, startidx, maxJoinChars);
        }
    }

    // handles EXT
    private String[] list(FsNodeChunk subNode, int startidx, int maxJoinChars) {
        if (subNode.get().type != FsNodeType.FOLDER) {
            // return the single filename
            String[] back = { subNode.get().name };
            return back;
        }
        // nodeChunk is a folder and we do not believe it needs "long" to count the files in it!
        int size = (int) subNode.get().size;
        if (startidx >= size) {
            return null;
        }
        ArrayList<String> entries = new ArrayList<>();
        int charCount = 0;
        int extCount = 0;
        int indexInExt = 0;
        FsNodeChunk getRefsIn = subNode;
        for (int i=startidx; i<size; i++) {
            indexInExt = i - extCount * DxramFsConfig.ref_ids_each_fsnode;
            if (indexInExt < DxramFsConfig.ref_ids_each_fsnode) {
                long entryChunkId = getRefsIn.get().refIds[indexInExt];
                FsNodeChunk entryChunk = new FsNodeChunk(entryChunkId);
                chunkS.get(entryChunk);
                if (maxJoinChars > 0) {
                    charCount += entryChunk.get().name.length() +1; // +1 for "/" in "join()"
                    if (charCount > maxJoinChars) break;
                } 
                entries.add(entryChunk.get().name);

            } else {
                getRefsIn = new FsNodeChunk(getRefsIn.get().forwardId);
                chunkS.get(getRefsIn);
                LOG.debug("list: Need Chunk [%s] as EXT FsNode", String.format("0x%X", getRefsIn.getID()));
                extCount++;
                i--;
            }
        }

        return entries.toArray(new String[entries.size()]);
    }


    // handles EXT
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

    // handles EXT
    private FsNode getFsNode(String path) {
        String[] pathparts = path.split("/");
        LOG.debug(String.join(" , ", pathparts));
        FsNodeChunk subNode = ROOTN;
        chunkS.get(subNode);
        if (path.length() == 0) {
            return null;
        } else if (ROOTN.get().size < 1) {
            return null;
        } else {
            for (int i=0; i < pathparts.length; i++) {
                long subChunkId = getIn(pathparts[i], subNode);
                if (subChunkId == ChunkID.INVALID_ID) return null;
                subNode = new FsNodeChunk(subChunkId);
                chunkS.get(subNode);
            }
            // for ended without return: thus the path+file must exists!
        }
        return subNode.get();
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
                
                // @todo EXT to handle more than ref_ids_each_fsnode entries !!!!!
                
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
            
            // @todo EXT to handle more than ref_ids_each_fsnode entries !!!!!
            // @todo delete FILE and handle folder and files in EXT fsNodes
            
        }
        return false;
    }

    // handles EXT
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


        // @todo EXT to handle more than ref_ids_each_fsnode entries !!
        
        
        int refSize = parentNode.get().refSize;
        parentNode.get().refIds[refSize] = newdir.getID();
        parentNode.get().size++;
        parentNode.get().refSize++;

        // @todo error handling
        chunkS.put(parentNode);
        return newdir.getID();
    }
    
    private long mkFile(String name, FsNodeChunk parentNode) {
        FsNodeChunk newf = new FsNodeChunk();
        newf.get().init();
        chunkS.create(newf);
        LOG.debug("Create %s on Chunk [%s]", name, String.format("0x%X", newf.getID()));
        
        newf.get().type = FsNodeType.FILE;
        newf.get().name = new String(name.getBytes(DxramFsConfig.STRING_STD_CHARSET));
        newf.get().backId = parentNode.getID();
        newf.get().forwardId = newf.getID();   // to self as dummy link
        newf.get().size = 0; // count the total bytes of the file!!
        newf.get().refSize = 1; // we create a single block with length 0
        // we increment the refSize of the file, if we need additional blocks
        // we use EXT, if refSize >= ref_ids_each_fsnode

        BlockinfoChunk binch = new BlockinfoChunk();
        binch.get().init();
        chunkS.create(binch);
        LOG.debug("Create Blockinfo on Chunk [%s]", String.format("0x%X", binch.getID()));
        newf.get().refIds[0] = binch.getID(); // add the first, single Blockinfo as chunkid
        chunkS.put(newf);
        
        binch.get().offset = 0;  // Offset of the block in the file (index/position of that block in the file)
        binch.get().length = 0; // store how many byte did we need from this block? INT -> 2GB int limit!?
        binch.get().corrupt = false;
        
        BlockChunk bloch = new BlockChunk();
        bloch.get().init();
        chunkS.create(bloch);
        LOG.debug("Create Block on Chunk [%s]", String.format("0x%X", bloch.getID()));

        binch.get().storageId = bloch.getID(); // to the BlockChunk id, where the data exists (only 1 id because no/0 replica)


        // @todo we have to get it from the nodeid of the chunk lookup and search the concrete values by BootService
        // -> we should fill this values dynamically on request!!
        
        short blockOwningPeer = lookS.getPrimaryPeer(bloch.getID());
        InetSocketAddress nodeDetail = bootS.getNodeAddress(blockOwningPeer);
        
        // @todo map dxram host+addr+port to dxnet host+addr+port !!!!!!
        
        binch.get().host = nodeDetail.getHostString(); // hostname or alternative the ip address
        binch.get().addr = nodeDetail.getAddress().getHostAddress(); // the ip address as string!
        binch.get().port = nodeDetail.getPort();
        
        LOG.debug(
            "BlockChunk [%s] is on %s (%s:%d)",
            String.format("0x%X", bloch.getID()),
            binch.get().host,
            binch.get().addr,
            binch.get().port
        );

        chunkS.put(binch);

        // @todo EXT to handle more than ref_ids_each_fsnode entries !!
        
        int refSize = parentNode.get().refSize;
        parentNode.get().refIds[refSize] = newf.getID();
        parentNode.get().size++;
        parentNode.get().refSize++;

        // @todo error handling
        chunkS.put(parentNode);
        return newf.getID();
    }

    // @todo: is this the correct behavior: /a/b exists and we want to move /c into /a/b but we did not get /a/b/c ! we get an error, that /a/b still exists!
    // ------> HINT: this error did not happend by using the hodoop dxramfs connector, because that code handles it!
    // ------> maybe the connector ask many "exists()" in that situation.
    // ------> for the future: test files rename!
    private String rename(String from, String to) {
        String back = "OK";
        if (from.length() == 0) return "fail. / not moveable.";
        if (exists(to).startsWith("OK")) return "fail. '"+to+"' still exists.";
        /* @todo if "to" is a folder, we have to move a folder or file into that folder and 
           check before, if this new structure exists,too!
        */
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
        
        // @todo EXT to handle more than ref_ids_each_fsnode entries !!!!!
        
        int refSize = toParentChunk.get().refSize;
        toParentChunk.get().refIds[refSize] = fromChunk.getID();
        toParentChunk.get().size++;
        toParentChunk.get().refSize++;

        // @todo error handling
        chunkS.put(toParentChunk);
        
        return back;
    }

    // ------------------------------------------------------------------------------------------------

    // handles EXT
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

    // handles EXT
    private IsDirectoryMessage externalHandleIsDirectory(IsDirectoryMessage msg) {
        IsDirectoryMessage response = new IsDirectoryMessage(msg.getSource(), isDir(msg.getData()));
        return response;
    }

    // handles EXT
    private FileLengthMessage externalHandleFileLength(FileLengthMessage msg) {
        String path = msg.get_data();
        long fileLength = 0;
        String back = "OK";

        String[] pathparts = path.split("/");
        LOG.debug(String.join(" , ", pathparts));
        chunkS.get(ROOTN);
        if (path.length() == 0) {
            back = "OK / is a dir";
        } else if (ROOTN.get().size < 1) {
            back = "OK / empty";
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
                    } else if (subNode.get().type == FsNodeType.FOLDER) {
                        fileLength = 0;
                        back = "OK but a folder";
                    } else {
                        back = "no: it is a EXT?!";
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

    // handles EXT
    private ListMessage externalHandleList(ListMessage msg) {
        String reqFolder = msg.getData();
        int reqStartIdx = msg.getCount();
        String[] contents = list(reqFolder, reqStartIdx, DxramFsConfig.max_pathlength_chars);
        if (contents == null || contents.length == 0) {
            return new ListMessage(msg.getSource(), "", -1);
        } else {
            // we return the (part of an) array with: "entry1/entry2/entry3/entry4/...."
            return new ListMessage(
                msg.getSource(),
                String.join("/", contents),
                contents.length
            );
        }
    }
    
    private CreateMessage externalHandleCreate(CreateMessage msg) {
        String back;
        String path = msg.getData();
        String[] pathparts = path.split("/");
        LOG.debug(String.join(" , ", pathparts));
        
        FsNodeChunk subNode = ROOTN;
        long subChunkId = ROOT_CID;

        if (path.length() > 0) {
            int i;
            for (i = 0; i < pathparts.length-1; i++) { // browse to folder
                subChunkId = getIn(pathparts[i], subNode);
                if (subChunkId == ChunkID.INVALID_ID) break;
                subNode = new FsNodeChunk(subChunkId);
                chunkS.get(subNode);
            }
            
            if (subChunkId == ChunkID.INVALID_ID) {
                back = "fail. path wrong.";
            } else {
                subChunkId = mkFile(pathparts[i], subNode);
                back = "OK.";
            }

        } else {
            back = "fail. name is empty";
        }

        CreateMessage response = new CreateMessage(msg.getSource(), back, subChunkId);
        return response;
    }

    // handles EXT
    private FsNodeMessage externalHandleFsNode(FsNodeMessage msg) {
        String back = "OK";
        String path = msg.get_data();
        FsNode fsnode = null;
        
        if (path.length() > 0) {
            fsnode = getFsNode(path);
            if (fsnode == null) back = "fail. path wrong.";
        } else {
            back = "fail. name is empty";
        }

        FsNodeMessage response = new FsNodeMessage(msg.getSource(), back);
        response.set_fsNode(fsnode);
        return response;
    }
    
    // does not need to handle EXT
    private FsNodeByIdMessage externalHandleFsNodeById(FsNodeByIdMessage msg) {
        String back = "OK";
        String chunkidstr = msg.get_data();
        FsNode fsnode = null;
        
        if (chunkidstr.length() > 0) {
            FsNodeChunk subNode = new FsNodeChunk(Long.valueOf(chunkidstr));
            chunkS.get(subNode);
            if (subNode.getID() == ChunkID.INVALID_ID) {
                back = "fail. id wrong.";
            } else {
                fsnode = subNode.get();
            }
        } else {
            back = "fail. id is empty";
        }

        FsNodeByIdMessage response = new FsNodeByIdMessage(msg.getSource(), back);
        response.set_fsNode(fsnode);
        return response;
    }
    
    // does not need to handle EXT
    private BlockinfoMessage externalHandleBlockinfo(BlockinfoMessage msg) {
        String back = "OK";
        String chunkidstr = msg.get_data();
        Blockinfo bi = null;
        
        if (chunkidstr.length() > 0) {
            BlockinfoChunk biChunk = new BlockinfoChunk(Long.valueOf(chunkidstr));
            chunkS.get(biChunk);
            if (biChunk.getID() == ChunkID.INVALID_ID) {
                back = "fail. id wrong.";
            } else {
                bi = biChunk.get();
            }
        } else {
            back = "fail. id is empty";
        }

        BlockinfoMessage response = new BlockinfoMessage(msg.getSource(), back);
        response.set_Blockinfo(bi);
        return response;
    }

}
