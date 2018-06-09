package de.hhu.bsinfo.dxramfs.core;

import java.util.ArrayList;

public class NodePeerConfig {

    public static class PeerConfig {
        public short nodeId;
        public String addr;
        public int port;
    }

    public short nodeId;
    public String addr;
    public int port;
    public ArrayList<PeerConfig> dxPeers;
}
