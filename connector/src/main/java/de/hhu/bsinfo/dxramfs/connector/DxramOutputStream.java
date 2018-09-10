package de.hhu.bsinfo.dxramfs.connector;
import de.hhu.bsinfo.dxnet.DXNet;

import java.io.IOException;
import java.io.OutputStream;

public class DxramOutputStream extends OutputStream {

    private DXNet _dxnet;
    private String _remotePath;
    /**
     * Constructor
     * @param remotePath is without the dxramfs://host:port part!
     */
    public DxramOutputStream(String remotePath, DXNet dxnet) {
        _remotePath = remotePath;
        _dxnet = dxnet;
    }

    @Override
    public void write(int b) throws IOException {

    }
}
