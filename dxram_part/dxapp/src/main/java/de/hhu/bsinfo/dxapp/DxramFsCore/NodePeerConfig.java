package de.hhu.bsinfo.dxapp.dxramfscore;

import java.util.ArrayList;

public class NodePeerConfig {

    public static class Mapping {
        public short nodeId;
        public String dxnet_addr;
        public int    dxnet_port;

        public String dxram_addr;
        public int    dxram_port;
    }

    // me !
    public short nodeId;
    public String dxnet_addr;
    public int    dxnet_port;
    public String dxram_addr;
    public int    dxram_port;

    public ArrayList<Mapping> peerMappings;

    public static NodePeerConfig factory(short myNodeId, String[] dxnet2dxramConf) {
        NodePeerConfig nopeConfig = new NodePeerConfig();
        nopeConfig.peerMappings = new ArrayList<>();

        for (String idNetPee : dxnet2dxramConf) {
            String[] id_Net_Pee = idNetPee.split("@");
            String[] host_port = id_Net_Pee[1].split(":");
            if (Short.valueOf(id_Net_Pee[0]) == myNodeId) {
                nopeConfig.nodeId = myNodeId;
                nopeConfig.dxnet_addr = host_port[0];
                nopeConfig.dxnet_port = Integer.valueOf(host_port[1]);
                if (id_Net_Pee.length == 3) {
                    String[] dxramhost_dxramport = id_Net_Pee[2].split(":");
                    nopeConfig.dxram_addr = dxramhost_dxramport[0];
                    nopeConfig.dxram_port = Integer.valueOf(dxramhost_dxramport[1]);
                }
            } else {
                NodePeerConfig.Mapping aPeer = new NodePeerConfig.Mapping();
                aPeer.nodeId = Short.valueOf(id_Net_Pee[0]);
                aPeer.dxnet_addr = host_port[0];
                aPeer.dxnet_port = Integer.valueOf(host_port[1]);
                if (id_Net_Pee.length == 3) {
                    String[] dxramhost_dxramport = id_Net_Pee[2].split(":");
                    aPeer.dxram_addr = dxramhost_dxramport[0];
                    aPeer.dxram_port = Integer.valueOf(dxramhost_dxramport[1]);
                }
                nopeConfig.peerMappings.add(aPeer);
            }
        }
        return nopeConfig;
    }

    public static NodePeerConfig factory(String dxramHost, String dxramAddr, int dxramPort, String[] dxnet2dxramConf) {
        NodePeerConfig nopeConfig = new NodePeerConfig();
        nopeConfig.peerMappings = new ArrayList<>();

        for (String idNetPee : dxnet2dxramConf) {
            String[] id_Net_Pee = idNetPee.split("@");
            String[] host_port = id_Net_Pee[1].split(":");
            String[] dxramhost_dxramport = null;
            if (id_Net_Pee.length == 3) dxramhost_dxramport = id_Net_Pee[2].split(":");

            if (
                    id_Net_Pee.length == 3 &&
                    Integer.valueOf(dxramhost_dxramport[1]) == dxramPort &&
                    ( dxramAddr.equals(dxramhost_dxramport[0]) || dxramHost.equals(dxramhost_dxramport[0]) )
            ) {

                nopeConfig.nodeId = Short.valueOf(id_Net_Pee[0]);
                nopeConfig.dxnet_addr = host_port[0];
                nopeConfig.dxnet_port = Integer.valueOf(host_port[1]);

                nopeConfig.dxram_addr = dxramhost_dxramport[0];
                nopeConfig.dxram_port = Integer.valueOf(dxramhost_dxramport[1]);

            } else {
                NodePeerConfig.Mapping aPeer = new NodePeerConfig.Mapping();
                aPeer.nodeId = Short.valueOf(id_Net_Pee[0]);
                aPeer.dxnet_addr = host_port[0];
                aPeer.dxnet_port = Integer.valueOf(host_port[1]);
                if (id_Net_Pee.length == 3) {
                    aPeer.dxram_addr = dxramhost_dxramport[0];
                    aPeer.dxram_port = Integer.valueOf(dxramhost_dxramport[1]);
                }
                nopeConfig.peerMappings.add(aPeer);
            }
        }
        return nopeConfig;
    }

    // @todo a fast mapping function for blocklocation data to get the dxnet peer of a dxram peer storage
};
