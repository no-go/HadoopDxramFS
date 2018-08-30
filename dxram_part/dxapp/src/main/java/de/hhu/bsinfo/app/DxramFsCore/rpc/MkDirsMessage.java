package de.hhu.bsinfo.app.dxramfscore.rpc;

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

import java.nio.charset.StandardCharsets;

public class MkDirsMessage extends Message {

    public static final Logger LOG = LogManager.getLogger(MkDirsMessage.class.getName());
    public static final byte MTYPE = 42;
    public static final byte TAG = 18;
    private byte[] _data;

    public static boolean gotResult;
    public static MkDirsMessage result;

    public String get_data() {
        return new String(_data, StandardCharsets.UTF_8);
    }

    @Override
    protected final int getPayloadLength() {
        return ObjectSizeUtil.sizeofByteArray(_data);
    }

    @Override
    protected final void writePayload(
            final AbstractMessageExporter p_exporter
    ) {
        p_exporter.writeByteArray(_data);
    }

    @Override
    protected final void readPayload(
            final AbstractMessageImporter p_importer
    ) {
        _data = p_importer.readByteArray(_data);
    }

    // ---------------------------------------------------------------

    public MkDirsMessage() {
        super();
    }

    public MkDirsMessage(final short p_destination) {
        super(p_destination, MkDirsMessage.MTYPE, MkDirsMessage.TAG);
        _data = new byte[DxramFsConfig.max_pathlength_chars];
    }

    public MkDirsMessage(final short p_destination, final String p_data) {
        super(p_destination, MkDirsMessage.MTYPE, MkDirsMessage.TAG);
        gotResult = false;
        _data = p_data.getBytes(StandardCharsets.UTF_8);
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
            LOG.debug("got Response: " + result.get_data());
            if (result.get_data().startsWith("OK")) {
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
            result = (MkDirsMessage) p_message;
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
