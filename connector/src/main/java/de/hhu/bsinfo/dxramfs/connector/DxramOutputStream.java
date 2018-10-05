package de.hhu.bsinfo.dxramfs.connector;
import de.hhu.bsinfo.app.dxramfscore.Block;
import de.hhu.bsinfo.app.dxramfscore.Blockinfo;
import de.hhu.bsinfo.app.dxramfscore.DxramFsConfig;
import de.hhu.bsinfo.app.dxramfscore.FsNode;
import de.hhu.bsinfo.app.dxramfscore.rpc.*;
import de.hhu.bsinfo.dxnet.DXNet;

import java.io.IOException;
import java.io.OutputStream;

/**
 * class to write data into correct file blocks. we use a block as buffer
 */
public class DxramOutputStream extends OutputStream {

    private DXNet _dxnet;
    private DxramFile _dxramFile;
    private String _remotePath;
    private FsNode _fsNode;
    private Blockinfo _blockinfo;
    private Block _block;

    public DxramOutputStream(DxramFile dxramFile, DXNet dxnet) throws IOException {
        _remotePath = dxramFile.getPathWithoutScheme();
        _dxramFile = dxramFile;
        _dxnet = dxnet;
        readTail();
    }

    private void readTail() throws IOException {
        FsNodeMessage fsnMsg = new FsNodeMessage(_dxramFile.getNearPeerId(), _remotePath);
        _fsNode = fsnMsg.send(_dxnet);
        if (_fsNode == null) {
            throw new IOException("get FsNode: something is really wrong");
        }

        // output is for write. the default is: write to the file end (?) -> wehave to load the last block

        int blockIndex = (int) ( _fsNode.size / (long) DxramFsConfig.file_blocksize);
        if (blockIndex >= DxramFsConfig.ref_ids_each_fsnode) {
            // @todo based on the fsNode data, we should load EXT FsNodeByIdMessage !!!!!!!!!!!!
        }

        BlockinfoMessage biMsg = new BlockinfoMessage(_dxramFile.getNearPeerId(), String.valueOf(_fsNode.refIds[blockIndex]));
        _blockinfo = biMsg.send(_dxnet);
        if (_blockinfo == null) {
            throw new IOException("get Blockinfo: something is really wrong");
        }

        // use blockid to load block into buffer
        AskBlockMessage msg = new AskBlockMessage(_dxramFile.getNearPeerId(),_blockinfo.ID);
        boolean res = msg.send(_dxnet);
        if (res) {
            _block = AskBlockMessage._result;
        } else {
            throw new IOException("ask+get Block: something is really wrong");
        }
    }

    @Override
    public void write(int b) throws IOException {
        if (_blockinfo.length == DxramFsConfig.file_blocksize) {
            // block is full
            flush();
            // get (maybe enlarged) new/next block and other data
            readTail();
        }
        _block._data[_blockinfo.length] = (byte) b;
        _blockinfo.length++;
        _fsNode.size++;
    }

    @Override
    public void write(byte[] dat) throws IOException {
        this.write(dat, 0, dat.length);
    }

    @Override
    public void write(byte[] dat, int startpos, int leng) throws IOException {
        if (dat == null) {
            throw new NullPointerException();
        } else if (startpos >= 0 && startpos <= dat.length && leng >= 0 && startpos + leng <= dat.length && startpos + leng >= 0) {
            if (leng != 0) {
                for(int var4 = 0; var4 < leng; ++var4) {
                    this.write(dat[startpos + var4]);
                }
            }
        } else {
            throw new IndexOutOfBoundsException();
        }
    }

    @Override
    public void flush() throws IOException {
        FlushMessage msg = new FlushMessage(_dxramFile.getNearPeerId(), _fsNode, _blockinfo, _block);
        boolean res = msg.send(_dxnet);
        if (!res) {
            throw new IOException("flush(): something is really wrong");
        }
    }

    @Override
    public void close() throws IOException {
        flush();
    }
}
