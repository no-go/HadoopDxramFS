package de.hhu.bsinfo.dxramfs.connector;
import de.hhu.bsinfo.app.dxramfscore.Block;
import de.hhu.bsinfo.app.dxramfscore.Blockinfo;
import de.hhu.bsinfo.dxnet.DXNet;

import java.io.IOException;
import java.io.OutputStream;

public class DxramOutputStream extends OutputStream {

    private DXNet _dxnet;
    private String _remotePath;
    private Block _block;
    private Blockinfo _blockinfo;

    /**
     * Constructor
     * @param remotePath is without the dxramfs://host:port part!
     */
    public DxramOutputStream(String remotePath, DXNet dxnet) {
        _remotePath = remotePath;
        _dxnet = dxnet;
        _block = new Block();
        _blockinfo = new Blockinfo();
        _block.init();
        _blockinfo.init();
        _blockinfo.length = 0;
    }

    @Override
    public void write(int b) throws IOException {
        _block._data[_blockinfo.length] = (byte) b;
        _blockinfo.length++;
        // @todo flush + get block or buffer
    }

    @Override
    public void write(byte[] var1) throws IOException {
        this.write(var1, 0, var1.length);
    }

    @Override
    public void write(byte[] var1, int var2, int var3) throws IOException {
        // @todo flush + get block or buffer
        if (var1 == null) {
            throw new NullPointerException();
        } else if (var2 >= 0 && var2 <= var1.length && var3 >= 0 && var2 + var3 <= var1.length && var2 + var3 >= 0) {
            if (var3 != 0) {
                for(int var4 = 0; var4 < var3; ++var4) {
                    this.write(var1[var2 + var4]);
                }

            }
        } else {
            throw new IndexOutOfBoundsException();
        }
    }

    @Override
    public void flush() throws IOException {
        // @todo send block or buffer
    }

    @Override
    public void close() throws IOException {
        flush();
    }
}
