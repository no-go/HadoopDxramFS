package de.hhu.bsinfo.hadoop.fs.dxnet;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import de.hhu.bsinfo.dxnet.DXNet;
import de.hhu.bsinfo.dxnet.DXNetConfig;
import de.hhu.bsinfo.dxnet.DXNetNodeMap;
import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxutils.StorageUnitGsonSerializer;
import de.hhu.bsinfo.dxutils.TimeUnitGsonSerializer;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;
import de.hhu.bsinfo.dxutils.unit.TimeUnit;
import org.apache.hadoop.conf.Configuration;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DxramFsPeer {
    public static final Logger LOG = LogManager.getLogger(DxramFsPeer.class.getName());

    private static int inCount = 0;

    public static short NODEID_dxnet_peer = 0;
    public static short NODEID_dxnet_Client = 1;

    public static void main(final String[] args) {
        System.out.println("Cwd: " + System.getProperty("user.dir"));

        Configuration hadoopCoreConf = new Configuration();
        hadoopCoreConf.addResource(args[0]);
        DXNet dxnet = setup(hadoopCoreConf, true);

        dxnet.registerMessageType(
                A100bMessage.MTYPE,
                A100bMessage.TAG,
                A100bMessage.class
        );
        dxnet.register(
                A100bMessage.MTYPE,
                A100bMessage.TAG,
                new DxramFsPeer.InHandler()
        );

        while (inCount < 1) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
                // nope.
            }
        }

        dxnet.close();
        System.exit(0);
    }

    public static DXNet setup(Configuration hadoopCoreConf, boolean iAmPeer) {
        DXNetConfig conf = new DXNetConfig();

        DXNetNodeMap nodeMap = null;

        short nodeId = Short.valueOf(hadoopCoreConf.get("dxnet.local.id"));
        NODEID_dxnet_Client = nodeId;
        NODEID_dxnet_peer = Short.valueOf(hadoopCoreConf.get("dxnet.local.peer.id"));
        if (iAmPeer) nodeId = NODEID_dxnet_peer;

        conf.getCoreConfig().setOwnNodeId(nodeId);
        nodeMap = new DXNetNodeMap(nodeId);

        nodeMap.addNode(
                Short.valueOf(hadoopCoreConf.get("dxnet.local.peer.id")),
                new InetSocketAddress(
                        hadoopCoreConf.get("dxnet.local.peer.addr"),
                        Integer.valueOf(hadoopCoreConf.get("dxnet.local.peer.port"))
                )
        );
        nodeMap.addNode(
                Short.valueOf(hadoopCoreConf.get("dxnet.local.id")),
                new InetSocketAddress(
                        hadoopCoreConf.get("dxnet.local.addr"),
                        Integer.valueOf(hadoopCoreConf.get("dxnet.local.port"))
                )
        );

        return new DXNet(
                conf.getCoreConfig(),
                conf.getNIOConfig(),
                conf.getIBConfig(),
                conf.getLoopbackConfig(),
                nodeMap
        );
    }

    public static class InHandler implements MessageReceiver {
        @Override
        public void onIncomingMessage(Message p_message) {
            A100bMessage eMsg = (A100bMessage) p_message;
            System.out.println(eMsg.toString());
            System.out.println(eMsg.getData());
            inCount++;
        }
    }
}
