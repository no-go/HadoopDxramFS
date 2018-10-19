package de.hhu.bsinfo.dxapp.dxramfscore.rpc;

import de.hhu.bsinfo.dxapp.dxramfscore.FsNodeType;
import de.hhu.bsinfo.dxapp.dxramfscore.DxramFsConfig;
import de.hhu.bsinfo.dxapp.dxramfscore.FsNode;
import de.hhu.bsinfo.dxnet.DXNet;
import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;

public class FsNodeMessage extends Message {

    public static final Logger LOG = LogManager.getLogger(FsNodeMessage.class.getName());
    public static final byte MTYPE = 42;
    public static final byte TAG = 21;
    private long _ID;
    private long _size;
    private int _type;
    private int _refSize;
    private long _backId;
    private long _forwardId;
    private byte[] _data;
    private byte[] _name;
    private long[] _refIds;

    public static boolean gotResult;
    public static FsNodeMessage result;

    public String get_data() {
        return new String(_data, DxramFsConfig.STRING_STD_CHARSET);
    }

    public final FsNode get_fsNode() {
        FsNode fsn = new FsNode();
        fsn.init();
        fsn.ID = _ID;
        fsn.size = _size;
        fsn.type = _type;
        fsn.refSize = _refSize;
        fsn.backId = _backId;
        fsn.forwardId = _forwardId;
        fsn.name = new String(_name, DxramFsConfig.STRING_STD_CHARSET);
        fsn.refIds = _refIds;
        return fsn;
    }

    /*
    public void set_fsNode(FsNode fsn) {
        this._ID = fsn.ID;
        this._size = fsn.size;
        this._type = fsn.type;
        this._refSize = fsn.refSize;
        this._backId = fsn.backId;
        this._forwardId = fsn.forwardId;

        //this._name = fsn.name.getBytes(DxramFsConfig.STRING_STD_CHARSET);
        //this._refIds = fsn.refIds;


        this._name = new byte[DxramFsConfig.max_filenamelength_chars];
        for (int i=0; i<DxramFsConfig.max_filenamelength_chars && i < fsn.name.getBytes(DxramFsConfig.STRING_STD_CHARSET).length; i++) {
            this._name[i] = fsn.name.getBytes(DxramFsConfig.STRING_STD_CHARSET)[i];
        }

        this._refIds = new long[DxramFsConfig.ref_ids_each_fsnode];
        for (int i=0; i<DxramFsConfig.ref_ids_each_fsnode; i++) {
            this._refIds[i] = fsn.refIds[i];
        }
    }
    */

    @Override
    protected final int getPayloadLength() {
        int s = 0;
        s += Long.BYTES;
        s += Long.BYTES;
        s += Integer.BYTES;
        s += Integer.BYTES;
        s += Long.BYTES;
        s += Long.BYTES;
        s += /*2+ DxramFsConfig.max_pathlength_chars; */ObjectSizeUtil.sizeofByteArray(_data);
        s += /*2+ DxramFsConfig.max_filenamelength_chars; */ObjectSizeUtil.sizeofByteArray(_name);
        s += /*2+ Long.BYTES * DxramFsConfig.ref_ids_each_fsnode; */ObjectSizeUtil.sizeofLongArray(_refIds);
        return s;
    }

    @Override
    protected final void writePayload(
            final AbstractMessageExporter p_exporter
    ) {
        p_exporter.writeLong(_ID);
        p_exporter.writeLong(_size);
        p_exporter.writeInt(_type);
        p_exporter.writeInt(_refSize);
        p_exporter.writeLong(_backId);
        p_exporter.writeLong(_forwardId);
        p_exporter.writeByteArray(_data);
        p_exporter.writeByteArray(_name);
        p_exporter.writeLongArray(_refIds);
    }

    @Override
    protected final void readPayload(
            final AbstractMessageImporter p_importer
    ) {
        _ID = p_importer.readLong(_ID);
        _size = p_importer.readLong(_size);
        _type = p_importer.readInt(_type);
        _refSize = p_importer.readInt(_refSize);
        _backId = p_importer.readLong(_backId);
        _forwardId = p_importer.readLong(_forwardId);
        _data = p_importer.readByteArray(_data);
        _name = p_importer.readByteArray(_name);
        _refIds = p_importer.readLongArray(_refIds);
    }

    // ---------------------------------------------------------------

    public FsNodeMessage() {
        super();
    }

    public FsNodeMessage(final short p_destination) {
        super(p_destination, FsNodeMessage.MTYPE, FsNodeMessage.TAG);
        _ID = DxramFsConfig.INVALID_ID;
        _size = 0;
        _type = FsNodeType.FILE;
        _refSize = 1;
        _backId = DxramFsConfig.INVALID_ID;
        _forwardId = DxramFsConfig.INVALID_ID;
        _data = new byte[DxramFsConfig.max_pathlength_chars];
        _name = new byte[DxramFsConfig.max_filenamelength_chars];
        _refIds = new long[DxramFsConfig.ref_ids_each_fsnode];
    }

    public FsNodeMessage(final short p_destination, final String _data) {
        super(p_destination, FsNodeMessage.MTYPE, FsNodeMessage.TAG);
        this._data = _data.getBytes(DxramFsConfig.STRING_STD_CHARSET);
        _ID = DxramFsConfig.INVALID_ID;
        _size = 0;
        _type = FsNodeType.FILE;
        _refSize = 1;
        _backId = DxramFsConfig.INVALID_ID;
        _forwardId = DxramFsConfig.INVALID_ID;
        _name = new byte[DxramFsConfig.max_filenamelength_chars];
        _refIds = new long[DxramFsConfig.ref_ids_each_fsnode];
    }

    public FsNodeMessage(
            final short p_destination,
            final String _data,
            final long _ID,
            final long _size,
            final int _type,
            final int _refSize,
            final long _backId,
            final long _forwardId,
            final String _name,
            final long[] _refIds
    ) {
        super(p_destination, FsNodeMessage.MTYPE, FsNodeMessage.TAG);
        gotResult = false;
        this._data = _data.getBytes(DxramFsConfig.STRING_STD_CHARSET);
        this._ID = _ID;
        this._size = _size;
        this._type = _type;
        this._refSize = _refSize;
        this._backId = _backId;
        this._forwardId = _forwardId;
        this._name = _name.getBytes(DxramFsConfig.STRING_STD_CHARSET);
        this._refIds = _refIds;
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
            LOG.debug("send payload: " + getPayloadLength());
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
                //LOG.debug(result.get_fsNode().name + " with size:" + result.get_fsNode().size);
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
