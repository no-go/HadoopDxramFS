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

    public static final short NODEID_dxnet_peer = 0;
    public static final short NODEID_dxnet_Client = 1;

    public static void main(final String[] args) {
        System.out.println("Cwd: " + System.getProperty("user.dir"));

        Configuration hadoopCoreConf = new Configuration();
        hadoopCoreConf.addResource(args[0]);
        DXNet dxnet = setup(hadoopCoreConf, NODEID_dxnet_peer);

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

    private static DXNetConfig readConfig(String filename) {
        DXNetConfig conf = new DXNetConfig();

        Gson gson = new GsonBuilder().
                setPrettyPrinting().
                excludeFieldsWithoutExposeAnnotation().
                registerTypeAdapter(
                        StorageUnit.class,
                        new StorageUnitGsonSerializer()
                ).registerTypeAdapter(
                TimeUnit.class,
                new TimeUnitGsonSerializer()
        ).create();

        try {
            JsonElement element = gson.fromJson(
                    new String(Files.readAllBytes(Paths.get(filename))),
                    JsonElement.class
            );
            conf = gson.fromJson(element, DXNetConfig.class);
        } catch (final Exception e) {
            System.exit(-1);
        }
        if (!conf.verify()) System.exit(-2);

        return conf;
    }

    public static DXNet setup(Configuration hadoopCoreConf, short ownNodeId) {
        String dxnetConfigFilename = hadoopCoreConf.get("dxnet.ConfigPath");
        DXNetConfig conf = readConfig(dxnetConfigFilename);

        conf.getCoreConfig().setOwnNodeId(ownNodeId);

        DXNetNodeMap nodeMap = new DXNetNodeMap(ownNodeId);
        DXNetConfig.NodeEntry peerNode = conf.getNodeList().get(NODEID_dxnet_peer);
        DXNetConfig.NodeEntry clientNode = conf.getNodeList().get(NODEID_dxnet_Client);
        nodeMap.addNode(
                NODEID_dxnet_peer,
                new InetSocketAddress(
                        peerNode.getAddress().getIP(),
                        peerNode.getAddress().getPort()
                )
        );
        nodeMap.addNode(
                NODEID_dxnet_Client,
                new InetSocketAddress(
                        clientNode.getAddress().getIP(),
                        clientNode.getAddress().getPort()
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
