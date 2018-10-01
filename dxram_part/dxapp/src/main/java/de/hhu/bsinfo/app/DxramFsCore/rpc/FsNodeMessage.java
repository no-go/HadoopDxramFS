package de.hhu.bsinfo.app.dxramfscore.rpc;

import de.hhu.bsinfo.dxnet.DXNet;
import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.app.dxramfscore.DxramFsConfig;
import de.hhu.bsinfo.app.dxramfscore.FsNode;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;

public class FsNodeMessage extends Message {

    public static final Logger LOG = LogManager.getLogger(FsNodeMessage.class.getName());
    public static final byte MTYPE = 42;
    public static final byte TAG = 21;
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
    public static FsNodeMessage result;

    public String get_data() {
        return new String(_data, DxramFsConfig.STRING_STD_CHARSET);
    }

    public FsNode get_fsNode() {
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

    public void set_fsNode(FsNode fsn) {
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

    // @todo: implement
    @Override
    protected final void writePayload(
            final AbstractMessageExporter p_exporter
    ) {
        // DxramFsConfig.ref_ids_each_fsnode
        p_exporter.writeLong(_size);
        p_exporter.writeByteArray(_data);
    }

    // @todo: implement
    @Override
    protected final void readPayload(
            final AbstractMessageImporter p_importer
    ) {
        // DxramFsConfig.ref_ids_each_fsnode
        _size = p_importer.readLong(_size);
        _data = p_importer.readByteArray(_data);
    }

    // ---------------------------------------------------------------

    public FsNodeMessage() {
        super();
    }

    public FsNodeMessage(final short p_destination) {
        super(p_destination, FsNodeMessage.MTYPE, FsNodeMessage.TAG);
        _data = new byte[DxramFsConfig.max_pathlength_chars];
    }

    public FsNodeMessage(final short p_destination, final String p_data) {
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
    public FsNode send(DXNet dxnet) {
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
                return result.get_fsNode();
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
            result = (FsNodeMessage) p_message;
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
