package de.hhu.bsinfo.app.dxramfscore.rpc;

import de.hhu.bsinfo.app.dxramfscore.Blockinfo;
import de.hhu.bsinfo.dxnet.DXNet;
import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.app.dxramfscore.DxramFsConfig;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class BlockinfoMessage extends Message {

    public static final Logger LOG = LogManager.getLogger(BlockinfoMessage.class.getName());
    public static final byte MTYPE = 42;
    public static final byte TAG = 10;
    private byte[] _data;

    private long _ID;
    private long _size;
    private byte[] _name;
    private int _type;
    private int _refSize;
    private long _backId;
    private long _forwardId;
    private long[] _refIds;

    public static boolean gotResult;
    public static BlockinfoMessage result;

    public String get_data() {
        return new String(_data, DxramFsConfig.STRING_STD_CHARSET);
    }

    public Blockinfo get_Blockinfo() {
        Blockinfo bi = new Blockinfo();
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

    public void set_Blockinfo(Blockinfo bi) {
        this._ID = fsn.ID;
        this._size = fsn.size;
        this._name = fsn.name.getBytes(DxramFsConfig.STRING_STD_CHARSET);
        this._type = fsn.type;
        this._refSize = fsn.refSize;
        this._backId = fsn.backId;
        this._forwardId = fsn.forwardId;
        this._refIds = fsn.refIds;
    }

    @Override
    protected final int getPayloadLength() {
        int s = ObjectSizeUtil.sizeofByteArray(_data);
        s += Long.BYTES;
        s += Long.BYTES;
        s += ObjectSizeUtil.sizeofByteArray(_name);
        s += Integer.BYTES;
        s += Integer.BYTES;
        s += Long.BYTES;
        s += Long.BYTES;
        s += ObjectSizeUtil.sizeofLongArray(_refIds);
        return s;
    }

    @Override
    protected final void writePayload(
            final AbstractMessageExporter p_exporter
    ) {
        p_exporter.writeByteArray(_data);
        p_exporter.writeLong(_ID);
        p_exporter.writeLong(_size);
        p_exporter.writeByteArray(_name);
        p_exporter.writeInt(_type);
        p_exporter.writeInt(_refSize);
        p_exporter.writeLong(_backId);
        p_exporter.writeLong(_forwardId);
        p_exporter.writeLongArray(_refIds);
    }

    @Override
    protected final void readPayload(
            final AbstractMessageImporter p_importer
    ) {
        _data = p_importer.readByteArray(_data);
        _ID = p_importer.readLong(_ID);
        _size = p_importer.readLong(_size);
        _name = p_importer.readByteArray(_name);
        _type = p_importer.readInt(_type);
        _refSize = p_importer.readInt(_refSize);
        _backId = p_importer.readLong(_backId);
        _forwardId = p_importer.readLong(_forwardId);
        _refIds = p_importer.readLongArray(_refIds);
    }

    // ---------------------------------------------------------------

    public BlockinfoMessage() {
        super();
    }

    public BlockinfoMessage(final short p_destination) {
        super(p_destination, FsNodeMessage.MTYPE, FsNodeMessage.TAG);
        _data = new byte[DxramFsConfig.max_pathlength_chars];
    }

    public BlockinfoMessage(final short p_destination, final String p_data) {
        super(p_destination, FsNodeMessage.MTYPE, FsNodeMessage.TAG);
        gotResult = false;
        _data = p_data.getBytes(DxramFsConfig.STRING_STD_CHARSET);
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
    public Blockinfo send(DXNet dxnet) {
        try {
            dxnet.sendMessage(this);
            while (gotResult == false) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {
                    // nothing.
                    // maybe handle response here !!
                }
            }
            LOG.debug("got Response: " + result.get_data());
            if (result.get_data().startsWith("OK")) {
                return result.get_Blockinfo();
            } else {
                return null;
            }
        } catch (NetworkException e) {
            e.printStackTrace();
            return null;
        }
    }


    public static class InHandler implements MessageReceiver {

        @Override
        public void onIncomingMessage(Message p_message) {
            result = (BlockinfoMessage) p_message;
            LOG.info(result.get_data());
            gotResult = true;
        }

        public Message Result() {
            return result;
        }

        public boolean gotResult() {
            return gotResult;
        }
    }
}
