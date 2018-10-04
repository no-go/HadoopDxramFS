package de.hhu.bsinfo.app.dxramfscore.rpc;

import de.hhu.bsinfo.app.dxramfscore.Block;
import de.hhu.bsinfo.app.dxramfscore.Blockinfo;
import de.hhu.bsinfo.app.dxramfscore.DxramFsConfig;
import de.hhu.bsinfo.app.dxramfscore.FsNode;
import de.hhu.bsinfo.dxnet.DXNet;
import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FlushMessage extends Message {

    public static final Logger LOG = LogManager.getLogger(FlushMessage.class.getName());
    public static final byte MTYPE = 42;
    public static final byte TAG = 23;

    // response handling

    public static FlushOkMessage result;
    public static boolean success;

    // submitted data:

    private long _ID;
    private long _size;
    private byte[] _name;
    private int _type;
    private int _refSize;
    private long _backId;
    private long _forwardId;
    private long[] _refIds;

    private long _biID;
    private int _offset;
    private int _length;
    private boolean _corrupt;
    private long _storageId;
    private byte[] _host;
    private byte[] _addr;
    private int _port;

    private long _bID;
    private byte[] _data;

    // getter and setter

    public FsNode getFsNode() {
        FsNode fsn = new FsNode();
        fsn.ID = _ID;
        fsn.size = _size;
        fsn.name = new String(_name, DxramFsConfig.STRING_STD_CHARSET);
        fsn.type = _type;
        fsn.refSize = _refSize;
        fsn.backId = _backId;
        fsn.forwardId = _forwardId;
        fsn.refIds = _refIds;
        return fsn;
    }

    public void setFsNode(FsNode fsn) {
        if (fsn == null) {
            fsn = new FsNode();
            fsn.init();
            fsn.ID = DxramFsConfig.INVALID_ID;
        }
        this._ID = fsn.ID;
        this._size = fsn.size;
        this._name = fsn.name.getBytes(DxramFsConfig.STRING_STD_CHARSET);
        this._type = fsn.type;
        this._refSize = fsn.refSize;
        this._backId = fsn.backId;
        this._forwardId = fsn.forwardId;
        this._refIds = fsn.refIds;
    }

    public Blockinfo getBlockinfo() {
        Blockinfo bi = new Blockinfo();
        bi.ID = _biID;
        bi.offset = _offset;
        bi.length = _length;
        bi.corrupt = _corrupt;
        bi.storageId = _storageId;
        bi.host = new String(_host, DxramFsConfig.STRING_STD_CHARSET);
        bi.addr = new String(_addr, DxramFsConfig.STRING_STD_CHARSET);
        bi.port = _port;
        return bi;
    }

    public void setBlockinfo(Blockinfo bi) {
        if (bi == null) {
            bi = new Blockinfo();
            bi.init();
            bi.ID = DxramFsConfig.INVALID_ID;
        }
        this._biID = bi.ID;
        this._offset = bi.offset;
        this._length = bi.length;
        this._corrupt = bi.corrupt;
        this._storageId = bi.storageId;
        this._host = bi.host.getBytes(DxramFsConfig.STRING_STD_CHARSET);
        this._addr = bi.addr.getBytes(DxramFsConfig.STRING_STD_CHARSET);
        this._port = bi.port;
    }

    public Block getBlock() {
        Block block = new Block();
        block.ID = _bID;
        block._data = _data;
        return block;
    }

    public void setBlock(Block b) {
        if (b == null) {
            b = new Block();
            b.init();
            b.ID = DxramFsConfig.INVALID_ID;
        }
        this._bID = b.ID;
        this._data = b._data;
    }

    // Data transfer handling

    @Override
    protected final int getPayloadLength() {
        int s = Long.BYTES;
        s += Long.BYTES;
        s += ObjectSizeUtil.sizeofByteArray(_name);
        s += Integer.BYTES;
        s += Integer.BYTES;
        s += Long.BYTES;
        s += Long.BYTES;
        s += ObjectSizeUtil.sizeofLongArray(_refIds);

        s += Long.BYTES;
        s += Integer.BYTES;
        s += Integer.BYTES;
        s += ObjectSizeUtil.sizeofBoolean();
        s += Long.BYTES;
        s += ObjectSizeUtil.sizeofByteArray(_host);
        s += ObjectSizeUtil.sizeofByteArray(_addr);
        s += Integer.BYTES;

        s += Long.BYTES;
        s += ObjectSizeUtil.sizeofByteArray(_data);
        return s;
    }

    @Override
    protected final void writePayload(
            final AbstractMessageExporter p_exporter
    ) {
        p_exporter.writeLong(_ID);
        p_exporter.writeLong(_size);
        p_exporter.writeByteArray(_name);
        p_exporter.writeInt(_type);
        p_exporter.writeInt(_refSize);
        p_exporter.writeLong(_backId);
        p_exporter.writeLong(_forwardId);
        p_exporter.writeLongArray(_refIds);

        p_exporter.writeLong(_biID);
        p_exporter.writeInt(_offset);
        p_exporter.writeInt(_length);
        p_exporter.writeBoolean(_corrupt);
        p_exporter.writeLong(_storageId);
        p_exporter.writeByteArray(_host);
        p_exporter.writeByteArray(_addr);
        p_exporter.writeInt(_port);

        p_exporter.writeLong(_bID);
        p_exporter.writeByteArray(_data);
    }

    @Override
    protected final void readPayload(
            final AbstractMessageImporter p_importer
    ) {
        _ID = p_importer.readLong(_ID);
        _size = p_importer.readLong(_size);
        _name = p_importer.readByteArray(_name);
        _type = p_importer.readInt(_type);
        _refSize = p_importer.readInt(_refSize);
        _backId = p_importer.readLong(_backId);
        _forwardId = p_importer.readLong(_forwardId);
        _refIds = p_importer.readLongArray(_refIds);

        _biID = p_importer.readLong(_biID);
        _offset = p_importer.readInt(_offset);
        _length = p_importer.readInt(_length);
        _corrupt = p_importer.readBoolean(_corrupt);
        _storageId = p_importer.readLong(_storageId);
        _host = p_importer.readByteArray(_host);
        _addr = p_importer.readByteArray(_addr);
        _port = p_importer.readInt(_port);

        _bID = p_importer.readLong(_bID);
        _data = p_importer.readByteArray(_data);
    }

    // ---------------------------------------------------------------

    public FlushMessage() {
        super();
    }

    public FlushMessage(final short p_destination) {
        super(p_destination, FlushMessage.MTYPE, FlushMessage.TAG);
        result = null;
        success = false;
        setFsNode(null);
        setBlockinfo(null);
        setBlock(null);
    }

    public FlushMessage(final short p_destination, final FsNode fsNode, final Blockinfo blockinfo, final Block block) {
        super(p_destination, FlushMessage.MTYPE, FlushMessage.TAG);
        result = null;
        success = false;
        setFsNode(fsNode);
        setBlockinfo(blockinfo);
        setBlock(block);
    }

    // ---------------------------------------------------------------

    /**
     * send a request for an existing path/file in DxramFs and handles the response.
     * Attention! The Request-Handling of this sended message is Outside of this Class!
     * The Request Handling can use methods of the InHandler to get access.
     *
     * @param dxnet
     * @return
     */
    public boolean send(DXNet dxnet) {
        try {
            FlushMessage.result = null;
            dxnet.sendMessage(this);
            while (FlushMessage.result == null) { // waiting for a FlushOkMessage
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) { }
            }
            return FlushMessage.success; // this success data was written by the FlushOkMessage Handler!

        } catch (NetworkException e) {
            e.printStackTrace();
            return false;
        }
    }


    // used by the dxramApp to handle incoming data
    public static class InHandler implements MessageReceiver {

        public FlushMessage dataMsg;

        @Override
        public void onIncomingMessage(Message p_message) {
            dataMsg = (FlushMessage) p_message;
        }
    }
}
