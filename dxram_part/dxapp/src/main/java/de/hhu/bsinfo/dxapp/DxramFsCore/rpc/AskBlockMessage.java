package de.hhu.bsinfo.dxapp.dxramfscore.rpc;

import de.hhu.bsinfo.dxapp.dxramfscore.Block;
import de.hhu.bsinfo.dxapp.dxramfscore.DxramFsConfig;
import de.hhu.bsinfo.dxnet.DXNet;
import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AskBlockMessage extends Message {

    public static final Logger LOG = LogManager.getLogger(AskBlockMessage.class.getName());
    public static final byte MTYPE = 42;
    public static final byte TAG = 20;
    private long _id;

    public static Block _result;
    public static boolean success;

    public long getAskBlockId() {
        return _id;
    }

    @Override
    protected final int getPayloadLength() {
        return Long.BYTES;
    }

    @Override
    protected final void writePayload(
            final AbstractMessageExporter p_exporter
    ) {
        p_exporter.writeLong(_id);
    }

    @Override
    protected final void readPayload(
            final AbstractMessageImporter p_importer
    ) {
        _id = p_importer.readLong(_id);
    }

    // ---------------------------------------------------------------

    public AskBlockMessage() {
        super();
    }

    public AskBlockMessage(final short p_destination) {
        super(p_destination, AskBlockMessage.MTYPE, AskBlockMessage.TAG);
        _id = DxramFsConfig.INVALID_ID;
        _result = null;
        success = false;
    }

    public AskBlockMessage(final short p_destination, final long id) {
        super(p_destination, AskBlockMessage.MTYPE, AskBlockMessage.TAG);
        _id = id;
        _result = null;
        success = false;
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
            AskBlockMessage._result = null;
            dxnet.sendMessage(this);
            while (AskBlockMessage._result == null) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {
                    // nothing.
                    // maybe handle response here !!
                }
            }
            LOG.debug("got Block by ask!");
            if (AskBlockMessage.success) {
                return true;
            } else {
                return false;
            }
        } catch (NetworkException e) {
            e.printStackTrace();
            return false;
        }
    }


    public static class InHandler implements MessageReceiver {

        public AskBlockMessage askMsg;

        @Override
        public void onIncomingMessage(Message p_message) {
            askMsg = (AskBlockMessage) p_message;
            LOG.info(askMsg._id);
        }
    }
}
