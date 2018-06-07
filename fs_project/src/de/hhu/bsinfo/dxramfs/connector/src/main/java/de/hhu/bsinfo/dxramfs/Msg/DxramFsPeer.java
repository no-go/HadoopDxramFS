package de.hhu.bsinfo.dxramfs.Msg;

import de.hhu.bsinfo.dxnet.DXNet;
import de.hhu.bsinfo.dxnet.DXNetConfig;
import de.hhu.bsinfo.dxnet.DXNetNodeMap;
import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.Message;
import org.apache.hadoop.conf.Configuration;

import java.net.InetSocketAddress;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/// @todo this will be obsolte - we integrate it in dxram
public class DxramFsPeer {
    public static final Logger LOG = LogManager.getLogger(DxramFsPeer.class.getName());

    private static int inCount = 0;

    public static short NODEID_dxnet_peer = 1;
    public static short NODEID_dxnet_Client = 0;

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

        while (inCount < 1000) {
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

        short nodeId = Short.valueOf(hadoopCoreConf.get("dxnet_local_id"));
        NODEID_dxnet_Client = nodeId;
        NODEID_dxnet_peer = Short.valueOf(hadoopCoreConf.get("dxnet_local_peer_id"));
        if (iAmPeer) nodeId = NODEID_dxnet_peer;

        System.out.println("get conf done");

        conf.getCoreConfig().setOwnNodeId(nodeId);
        System.out.println("set own node id done");

        nodeMap = new DXNetNodeMap(nodeId);
        System.out.println("nodemap done");

        nodeMap.addNode(
                Short.valueOf(hadoopCoreConf.get("dxnet_local_peer_id")),
                new InetSocketAddress(
                        hadoopCoreConf.get("dxnet_local_peer_addr"),
                        Integer.valueOf(hadoopCoreConf.get("dxnet_local_peer_port"))
                )
        );
        nodeMap.addNode(
                Short.valueOf(hadoopCoreConf.get("dxnet_local_id")),
                new InetSocketAddress(
                        hadoopCoreConf.get("dxnet_local_addr"),
                        Integer.valueOf(hadoopCoreConf.get("dxnet_local_port"))
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
            System.out.println(Integer.toString(inCount) + ": " + eMsg.getData());
            inCount++;
        }
    }
}
