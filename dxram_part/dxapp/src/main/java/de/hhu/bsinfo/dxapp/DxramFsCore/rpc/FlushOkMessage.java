package de.hhu.bsinfo.dxapp.dxramfscore.rpc;

import de.hhu.bsinfo.dxapp.dxramfscore.Block;
import de.hhu.bsinfo.dxapp.dxramfscore.DxramFsConfig;
import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FlushOkMessage extends Message {

    public static final Logger LOG = LogManager.getLogger(FlushOkMessage.class.getName());
    public static final byte MTYPE = 42;
    public static final byte TAG = 24;

    private boolean success;

    public boolean getSuccess() {
        return success;
    }

    @Override
    protected final int getPayloadLength() {
        return ObjectSizeUtil.sizeofBoolean();
    }

    @Override
    protected final void writePayload(
            final AbstractMessageExporter p_exporter
    ) {
        p_exporter.writeBoolean(success);
    }

    @Override
    protected final void readPayload(
            final AbstractMessageImporter p_importer
    ) {
        success = p_importer.readBoolean(success);
    }

    // ---------------------------------------------------------------

    public FlushOkMessage() {
        super();
    }

    public FlushOkMessage(final short p_destination) {
        super(p_destination, FlushOkMessage.MTYPE, FlushOkMessage.TAG);
        success = false;
    }

    public FlushOkMessage(final short p_destination, final boolean p_success) {
        super(p_destination, FlushOkMessage.MTYPE, FlushOkMessage.TAG);
        success = p_success;
    }

    // ---------------------------------------------------------------

    // used by the dxnet/hadoop connector to identify incoming OK messages after sending FSNode,Blockinfo and Block to dxramFsApp
    public static class InHandler implements MessageReceiver {

        @Override
        public void onIncomingMessage(Message p_message) {
            FlushOkMessage result = (FlushOkMessage) p_message;
            FlushMessage.success = result.getSuccess();
            FlushMessage.result = result;
        }
    }
}
