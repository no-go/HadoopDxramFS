package de.hhu.bsinfo.dxnet;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxnet.core.AbstractPipeIn;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;

import de.hhu.bsinfo.app.dxramfscore.DxramFsConfig;
import de.hhu.bsinfo.app.dxramfscore.*;
import de.hhu.bsinfo.app.dxramfscore.rpc.*;

public final class DXNetMain {
    private static final Logger LOG = LogManager.getFormatterLogger(DXNetMain.class.getSimpleName());

    private static DXNet _dxn;
    public static NodePeerConfig nopeConfig;
    private final static short MYPEERID = 0;
    private final static short CONNECTTOPEERID = 1;
    
    public static void main(final String[] p_arguments) {
        Locale.setDefault(new Locale("en", "US"));
        
        DxramFsConfig.file_blocksize = Integer.valueOf("4194304");
        DxramFsConfig.ref_ids_each_fsnode = Integer.valueOf("128");
        DxramFsConfig.max_pathlength_chars = Integer.valueOf("512");

        DxramFsConfig.max_filenamelength_chars = Integer.valueOf("128");
        DxramFsConfig.max_hostlength_chars = Integer.valueOf("80");
        DxramFsConfig.max_addrlength_chars = Integer.valueOf("48");
        
        
        nopeConfig = NodePeerConfig.factory(MYPEERID, "0@127.0.0.1:65220@,1@127.0.0.1:65221@127.0.0.1:22222,2@127.0.0.1:65222@127.0.0.1:22223,3@127.0.0.1:65223@".split(","));
        DxnetInit dxini = new DxnetInit(nopeConfig, nopeConfig.nodeId);
        _dxn = dxini.getDxNet();
        
        //------------------------------------------------------------------------------------------------------------
        
        
        MkDirsMessage mkdirMsg = new MkDirsMessage(CONNECTTOPEERID, "/folder/");
        boolean mkdirRes = mkdirMsg.send(_dxn);
        LOG.debug("mkdirs: " + String.valueOf(mkdirRes));
        
        CreateMessage cmsg1 = new CreateMessage(CONNECTTOPEERID, "/folder/test1.txt");
        boolean cr1Res = cmsg1.send(_dxn);
        LOG.debug("create empty file: " + String.valueOf(cr1Res));
        
        CreateMessage cmsg2 = new CreateMessage(CONNECTTOPEERID, "/folder/test2.txt");
        boolean cr2Res = cmsg2.send(_dxn);
        LOG.debug("create empty file: " + String.valueOf(cr2Res));
        
        
        ArrayList<String> fArrayList = new ArrayList<>();

        // do requests (and count received elements to get new start index), until we get null (nothing/empty)
        int startidx = 0;

        while (startidx != -1) {
            ListMessage msg = new ListMessage(
                    CONNECTTOPEERID,
                    "/folder/",
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
        
        //------------------------------------------------------------------------------------------------------------
        _dxn.close();
        System.exit(0);
    }

}
