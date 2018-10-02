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
    private int _offset;
    private int _length;
    private boolean _corrupt;
    private long _storageId;
    private byte[] _host;
    private byte[] _addr;
    private int _port;

    public static boolean gotResult;
    public static BlockinfoMessage result;

    public String get_data() {
        return new String(_data, DxramFsConfig.STRING_STD_CHARSET);
    }

    public Blockinfo get_Blockinfo() {
        Blockinfo bi = new Blockinfo();
        bi.ID = _ID;
        bi.offset = _offset;
        bi.length = _length;
        bi.corrupt = _corrupt;
        bi.storageId = _storageId;
        bi.host = new String(_host, DxramFsConfig.STRING_STD_CHARSET);
        bi.addr = new String(_addr, DxramFsConfig.STRING_STD_CHARSET);
        bi.port = _port;
        return bi;
    }

    public void set_Blockinfo(Blockinfo bi) {
        if (bi == null) {
            bi = new Blockinfo();
            bi.init();
            bi.ID = DxramFsConfig.INVALID_ID;
        }
        this._ID = bi.ID;
        this._offset = bi.offset;
        this._length = bi.length;
        this._corrupt = bi.corrupt;
        this._storageId = bi.storageId;
        this._host = bi.host.getBytes(DxramFsConfig.STRING_STD_CHARSET);
        this._addr = bi.addr.getBytes(DxramFsConfig.STRING_STD_CHARSET);
        this._port = bi.port;
    }

    @Override
    protected final int getPayloadLength() {
        int s = ObjectSizeUtil.sizeofByteArray(_data);
        s += Long.BYTES;
        s += Integer.BYTES;
        s += Integer.BYTES;
        s += Integer.BYTES; // boolean
        s += Long.BYTES;
        s += ObjectSizeUtil.sizeofByteArray(_host);
        s += ObjectSizeUtil.sizeofByteArray(_addr);
        s += Integer.BYTES;
        return s;
    }

    @Override
    protected final void writePayload(
            final AbstractMessageExporter p_exporter
    ) {
        p_exporter.writeByteArray(_data);
        p_exporter.writeLong(_ID);
        p_exporter.writeInt(_offset);
        p_exporter.writeInt(_length);
        p_exporter.writeBoolean(_corrupt);
        p_exporter.writeLong(_storageId);
        p_exporter.writeByteArray(_host);
        p_exporter.writeByteArray(_addr);
        p_exporter.writeInt(_port);
    }

    @Override
    protected final void readPayload(
            final AbstractMessageImporter p_importer
    ) {
        _data = p_importer.readByteArray(_data);
        _ID = p_importer.readLong(_ID);
        _offset = p_importer.readInt(_offset);
        _length = p_importer.readInt(_length);
        _corrupt = p_importer.readBoolean(_corrupt);
        _storageId = p_importer.readLong(_storageId);
        _host = p_importer.readByteArray(_host);
        _addr = p_importer.readByteArray(_addr);
        _port = p_importer.readInt(_port);
    }

    // ---------------------------------------------------------------

    public BlockinfoMessage() {
        super();
    }

    public BlockinfoMessage(final short p_destination) {
        super(p_destination, FsNodeMessage.MTYPE, FsNodeMessage.TAG);
        _data = new byte[DxramFsConfig.max_pathlength_chars];
        set_Blockinfo(null);
    }

    public BlockinfoMessage(final short p_destination, final String p_data) {
        super(p_destination, FsNodeMessage.MTYPE, FsNodeMessage.TAG);
        gotResult = false;
        _data = p_data.getBytes(DxramFsConfig.STRING_STD_CHARSET);
        set_Blockinfo(null);
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
