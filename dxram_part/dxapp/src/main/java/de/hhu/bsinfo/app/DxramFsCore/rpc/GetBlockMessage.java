package de.hhu.bsinfo.app.dxramfscore.rpc;

import de.hhu.bsinfo.app.dxramfscore.Block;
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

public class GetBlockMessage extends Message {

    public static final Logger LOG = LogManager.getLogger(GetBlockMessage.class.getName());
    public static final byte MTYPE = 42;
    public static final byte TAG = 18;

    private long bID;
    private byte[] bdata;
    private boolean _success;

    public static boolean gotResult;
    public static GetBlockMessage result;

    public Block getData() {
        Block _block = new Block();
        _block.ID = bID;
        _block._data = bdata;
        return _block;
    }

    public boolean getSuccess() {
        return _success;
    }

    public void setSuccess(boolean val) {
        _success = val;
    }

    @Override
    protected final int getPayloadLength() {
        return (Long.BYTES + ObjectSizeUtil.sizeofByteArray(bdata) + ObjectSizeUtil.sizeofBoolean());
    }

    @Override
    protected final void writePayload(
            final AbstractMessageExporter p_exporter
    ) {
        p_exporter.writeLong(bID);
        p_exporter.writeByteArray(bdata);
        p_exporter.writeBoolean(_success);
    }

    @Override
    protected final void readPayload(
            final AbstractMessageImporter p_importer
    ) {
        bID = p_importer.readLong(bID);
        bdata = p_importer.readByteArray(bdata);
        _success = p_importer.readBoolean(_success);
    }

    // ---------------------------------------------------------------

    public GetBlockMessage() {
        super();
    }


    // @todo: response Message (requestBlockMassage, responseBlockMessageS)

    public GetBlockMessage(final short p_destination) {
        super(p_destination, GetBlockMessage.MTYPE, GetBlockMessage.TAG);
        gotResult = false;
        bID = DxramFsConfig.INVALID_ID;
        bdata = new byte[DxramFsConfig.file_blocksize];
        _success = false;
    }

    public GetBlockMessage(final short p_destination, final Block p_data) {
        super(p_destination, GetBlockMessage.MTYPE, GetBlockMessage.TAG);
        gotResult = false;
        bID = p_data.ID;
        bdata = p_data._data;
        _success = false;
    }

    // ---------------------------------------------------------------

    public static class InHandler implements MessageReceiver {

        @Override
        public void onIncomingMessage(Message p_message) {
            result = (GetBlockMessage) p_message;
            LOG.info("get block");
            AskBlockMessage.success = result.getSuccess();
            AskBlockMessage._result = result.getData();
        }
    }
}
