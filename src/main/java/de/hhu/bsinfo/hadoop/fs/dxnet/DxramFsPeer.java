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

public class DxramFsPeer {

    private static int inCount = 0;

    public static void main(final String[] args) {
        System.out.println("Cwd: " + System.getProperty("user.dir"));

        //DXNetConfig ms_conf = readConfig(args[0]);
        DXNet ms_dxnet = setup(args[0]);

        ms_dxnet.registerMessageType(
                A100bMessage.MTYPE,
                A100bMessage.TAG,
                A100bMessage.class
        );
        ms_dxnet.register(
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

        ms_dxnet.close();
        System.exit(0);
    }

    /*
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
    */

    private static DXNet setup(String filename) {
        DXNetConfig conf = new DXNetConfig();
        Configuration hadoopCoreConf = new Configuration();
        hadoopCoreConf.addResource(filename);
        short nodeId = Short.valueOf(hadoopCoreConf.get("dxnet.local_id"));
        short peerNodeId = Short.valueOf(hadoopCoreConf.get("dxnet.local_peer_id"));

        conf.getCoreConfig().setOwnNodeId(peerNodeId);

        DXNetNodeMap nodeMap = new DXNetNodeMap(peerNodeId);
        nodeMap.addNode(peerNodeId, new InetSocketAddress(
                hadoopCoreConf.get("dxnet.local_peer_addr"),
                Integer.valueOf(hadoopCoreConf.get("dxnet.local_peer_port"))
        ));
        nodeMap.addNode(nodeId, new InetSocketAddress(
                hadoopCoreConf.get("dxnet.local_addr"),
                Integer.valueOf(hadoopCoreConf.get("dxnet.local_port"))
        ));
        System.out.println("my dxnet addr: " + hadoopCoreConf.get("dxnet.local_peer_addr"));

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
