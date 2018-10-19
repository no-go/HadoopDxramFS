package de.hhu.bsinfo.dxapp.dxramfscore.rpc;

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

import java.nio.charset.StandardCharsets;

public class RenameToMessage extends Message {

    public static final Logger LOG = LogManager.getLogger(RenameToMessage.class.getName());
    public static final byte MTYPE = 42;
    public static final byte TAG = 19;
    private byte[] data;
    private byte[] toData;

    public static boolean gotResult;
    public static RenameToMessage result;

    public String getData() {
        return new String(data, DxramFsConfig.STRING_STD_CHARSET);
    }
    public String getToData() {
        return new String(toData, DxramFsConfig.STRING_STD_CHARSET);
    }

    @Override
    protected final int getPayloadLength() {
        return ObjectSizeUtil.sizeofByteArray(data) + ObjectSizeUtil.sizeofByteArray(toData);
    }

    @Override
    protected final void writePayload(
            final AbstractMessageExporter p_exporter
    ) {
        p_exporter.writeByteArray(data);
        p_exporter.writeByteArray(toData);
    }

    @Override
    protected final void readPayload(
            final AbstractMessageImporter p_importer
    ) {
        data = p_importer.readByteArray(data);
        toData = p_importer.readByteArray(toData);
    }

    // ---------------------------------------------------------------

    public RenameToMessage() {
        super();
    }

    public RenameToMessage(final short p_destination) {
        super(p_destination, RenameToMessage.MTYPE, RenameToMessage.TAG);
        data = new byte[DxramFsConfig.max_pathlength_chars];
        toData = new byte[0];
    }

    public RenameToMessage(final short p_destination, final String p_data, final String to_data) {
        super(p_destination, RenameToMessage.MTYPE, RenameToMessage.TAG);
        gotResult = false;
        data = p_data.getBytes(DxramFsConfig.STRING_STD_CHARSET);
        toData = to_data.getBytes(DxramFsConfig.STRING_STD_CHARSET);
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
            dxnet.sendMessage(this);
            while (gotResult == false) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {
                    // nothing.
                    // maybe handle response here !!
                }
            }
            LOG.debug("got Response: " + result.getData());
            if (result.getData().startsWith("OK")) {
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

        @Override
        public void onIncomingMessage(Message p_message) {
            result = (RenameToMessage) p_message;
            LOG.info(result.getData());
            LOG.info(result.getToData());
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
