package de.hhu.bsinfo.hadoop.fs.dxnet;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import de.hhu.bsinfo.dxnet.DXNet;
import de.hhu.bsinfo.dxnet.DXNetConfig;
import de.hhu.bsinfo.dxnet.DXNetNodeMap;
import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxutils.StorageUnitGsonSerializer;
import de.hhu.bsinfo.dxutils.TimeUnitGsonSerializer;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;
import de.hhu.bsinfo.dxutils.unit.TimeUnit;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;

public class DxramFsClient {

    public static void main(final String[] args) {

        DXNetConfig ms_conf = readConfig(args[0]);
        DXNet ms_dxnet = setup(ms_conf, (short) 1, (short) 0);

        ms_dxnet.registerMessageType(
                A100bMessage.MTYPE,
                A100bMessage.TAG,
                A100bMessage.class
        );
        ms_dxnet.register(
                A100bMessage.MTYPE,
                A100bMessage.TAG,
                new DxramFsClient.InHandler()
        );

        A100bMessage msg = new A100bMessage(
                (short) 0, // to server
                new String("Hallo Welt")
        );

        try {
            ms_dxnet.sendMessage(msg);
        } catch (NetworkException e) {
            e.printStackTrace();
        }

        ms_dxnet.close();
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

    private static DXNet setup(DXNetConfig conf, short ownNodeId, short serverNodeId) {
        conf.getCoreConfig().setOwnNodeId(ownNodeId);

        DXNetNodeMap nodeMap = new DXNetNodeMap(ownNodeId);
        DXNetConfig.NodeEntry clientNode = conf.getNodeList().get(ownNodeId);
        DXNetConfig.NodeEntry serverNode = conf.getNodeList().get(serverNodeId);
        nodeMap.addNode(
                ownNodeId,
                new InetSocketAddress(
                        clientNode.getAddress().getIP(),
                        clientNode.getAddress().getPort()
                )
        );
        nodeMap.addNode(
                serverNodeId,
                new InetSocketAddress(
                        serverNode.getAddress().getIP(),
                        serverNode.getAddress().getPort()
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
        }
    }
}
