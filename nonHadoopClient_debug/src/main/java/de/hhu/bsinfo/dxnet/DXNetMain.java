package de.hhu.bsinfo.dxnet;

//import java.io.File;
//import java.io.FileNotFoundException;
//import java.io.IOException;
//import java.nio.file.Files;
//import java.nio.file.Paths;
import java.util.ArrayList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;

import de.hhu.bsinfo.dxapp.dxramfscore.DxramFsConfig;
import de.hhu.bsinfo.dxapp.dxramfscore.*;
import de.hhu.bsinfo.dxapp.dxramfscore.rpc.*;

public final class DXNetMain {
    private static final Logger LOG = LogManager.getFormatterLogger(DXNetMain.class.getSimpleName());

    private static DXNet _dxn;
    public static NodePeerConfig nopeConfig;
    private final static short MYPEERID = 0;
    private final static short CONNECTTOPEERID = 1;

    private static FsNode _fsNode;
    private static Blockinfo _blockinfo;
    private static Block _block;

    public static void main(final String[] arguments) {
        
        DxramFsConfig.file_blocksize = Integer.valueOf("4194304");
        DxramFsConfig.ref_ids_each_fsnode = Integer.valueOf("128");
        DxramFsConfig.max_pathlength_chars = Integer.valueOf("512");

        DxramFsConfig.max_filenamelength_chars = Integer.valueOf("128");
        DxramFsConfig.max_hostlength_chars = Integer.valueOf("80");
        DxramFsConfig.max_addrlength_chars = Integer.valueOf("48");
        
        
        nopeConfig = NodePeerConfig.factory(MYPEERID, "0@127.0.0.1:65220@,1@127.0.0.1:65221@127.0.0.1:22222,2@127.0.0.1:65222@127.0.0.1:22223,3@127.0.0.1:65223@".split(","));
        DxnetInit dxini = new DxnetInit(nopeConfig, nopeConfig.nodeId);
        _dxn = dxini.getDxNet();
        
        //---------------------------------------------------------------------------------------
        
        
        mkdirs("/folder/");
        
        create("/folder/test1.txt");
        create("/folder/test2.txt");
        //rename("/folder/", "/wubda/");
        //list("/wubda");
        list("/folder");
        
        
        _fsNode = readFsNode("/folder/test1.txt");
        LOG.debug("fsnode first blockinfo id: [%s]", String.format("0x%X", _fsNode.refIds[0]));
        
        _blockinfo = readBlockinfo(_fsNode, 0);
        _block = readBlock(_blockinfo);
        LOG.debug("block length: " + String.valueOf(_block._data.length));
        LOG.debug("block bytes: {} ...", new String(_block._data).substring(0, 25));
        
        
        delete("/folder/test1.txt");
        list("/folder");
        
        /*
        delete("/wubda/test2.txt");
        list("/wubda");
        delete("/wubda/");
        list("/");
        */
        //---------------------------------------------------------------------------------------
        _dxn.close();
        System.exit(0);
    }

    public static boolean mkdirs(String path) {
        MkDirsMessage mkdirMsg = new MkDirsMessage(CONNECTTOPEERID, path);
        boolean mkdirRes = mkdirMsg.send(_dxn);
        LOG.debug("mkdirs: " + String.valueOf(mkdirRes));
        return mkdirRes;
    }
    
    public static boolean create(String path) {
        CreateMessage cmsg1 = new CreateMessage(CONNECTTOPEERID, path);
        boolean cr1Res = cmsg1.send(_dxn);
        LOG.debug("create empty file: " + String.valueOf(cr1Res));
        return cr1Res;
    }
    
    public static boolean delete(String path) {
        DeleteMessage msg = new DeleteMessage(CONNECTTOPEERID, path);
        boolean res = msg.send(_dxn);
        LOG.debug("delete: " + String.valueOf(res));
        return res;
    }

    public static void list(String path) {
        ArrayList<String> fArrayList = new ArrayList<>();
        // do requests (and count received elements to get new start index), until we get null (nothing/empty)
        int startidx = 0;
        while (startidx != -1) {
            ListMessage msg = new ListMessage(
                    CONNECTTOPEERID,
                    path,
                    startidx
            );
            String[] listing = msg.send(_dxn);
            if (listing == null) {
                startidx = -1; // end while loop
            } else {
                startidx += listing.length;
                for (String f : listing) {
                    fArrayList.add(f);
                    LOG.info(f);
                }
            }
        }
    }

    // @todo: only works with folders ?!?!
    public static boolean rename(String from, String to) {
        RenameToMessage msg = new RenameToMessage(CONNECTTOPEERID, from, to);
        boolean res = msg.send(_dxn);
        LOG.debug("renameTo msg Response: " + String.valueOf(res));
        return res;
    }
    
    public static FsNode readFsNode(String path) {
        FsNodeMessage fsnMsg = new FsNodeMessage(CONNECTTOPEERID, path);
        return fsnMsg.send(_dxn);
    }
    
    public static Blockinfo readBlockinfo(FsNode fsn, int index) {
        BlockinfoMessage biMsg = new BlockinfoMessage(CONNECTTOPEERID, String.valueOf(fsn.refIds[index]));
        return biMsg.send(_dxn);
    }
    
    public static Block readBlock(Blockinfo bi) {
        AskBlockMessage msg = new AskBlockMessage(CONNECTTOPEERID, bi.ID);
        boolean res = msg.send(_dxn);
        LOG.debug("Block Response: " + String.valueOf(res));
        if (res) {
            return AskBlockMessage._result;
        } else {
            return null;
        }
    }
    
}
