package de.hhu.bsinfo.dxramfs.connector;
import de.hhu.bsinfo.app.dxramfscore.Block;
import de.hhu.bsinfo.app.dxramfscore.Blockinfo;
import de.hhu.bsinfo.app.dxramfscore.DxramFsConfig;
import de.hhu.bsinfo.app.dxramfscore.FsNode;
import de.hhu.bsinfo.app.dxramfscore.rpc.AskBlockMessage;
import de.hhu.bsinfo.app.dxramfscore.rpc.FsNodeMessage;
import de.hhu.bsinfo.dxnet.DXNet;

import java.io.IOException;
import java.io.OutputStream;

/**
 * class to write data into correct file blocks. we use a block as buffer
 */
public class DxramOutputStream extends OutputStream {

    private DXNet _dxnet;
    private String _remotePath;
    private FsNode _fsNode;
    private Blockinfo _blockinfo;
    private Block _block;

    public DxramOutputStream(DxramFile dxramFile, DXNet dxnet) throws IOException {
        _remotePath = dxramFile.getPathWithoutScheme();
        _dxnet = dxnet;

        FsNodeMessage fsnMsg = new FsNodeMessage(dxramFile.getNearPeerId(), _remotePath);
        _fsNode = fsnMsg.send(_dxnet);
        if (_fsNode == null) {
            throw new IOException("something is really wrong");
        }

        // output is for write. the default is: write to the file end (?) -> wehave to load the last block

        int blockIndex = (int) ( _fsNode.size / (long) DxramFsConfig.file_blocksize);
        if (blockIndex >= DxramFsConfig.ref_ids_each_fsnode) {
            // @todo based on the fsNode data, we should load EXT FsNodeByIdMessage !!!!!!!!!!!!
        } else {

        }



        // @todo get fsnode ?!   code is still a testing dummy !!! +++++++++++++++++++++++++++++

        _block = new Block();
        _blockinfo = new Blockinfo();
        _block.init();
        _blockinfo.init();
        _blockinfo.length = 0;

        // get blockid by calculation and filename via Blocklocations ?!

        // use blockid to load block into buffer
        AskBlockMessage msg = new AskBlockMessage(DxramFileSystem.nopeConfig.peerMappings.get(0).nodeId,0xB1BD000000000007l);
        boolean res = msg.send(_dxnet);
        if (res) {
            _block = AskBlockMessage._result;
        } else {
            //throw new IOException("something is really wrong");
        }


    }

    @Override
    public void write(int b) throws IOException {
        // fill buffer
        _block._data[_blockinfo.length] = (byte) b;
        _blockinfo.length++;
        // @todo flush (add/put block) + get block or buffer
    }

    @Override
    public void write(byte[] dat) throws IOException {
        // @todo we have to write on which position in the block???!!
        this.write(dat, 0, dat.length);
    }

    @Override
    public void write(byte[] dat, int startpos, int leng) throws IOException {
        // @todo flush (add/put block) + get block or buffer
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
        // @todo flush (add/put block) + get block or buffer

        // @todo we have to update fsnode, too! maybe with the block update via dxramApp
    }

    @Override
    public void close() throws IOException {
        flush();
    }
}
