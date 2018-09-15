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

public class ListMessage extends Message {

    public static final Logger LOG = LogManager.getLogger(ListMessage.class.getName());
    public static final byte MTYPE = 42;
    public static final byte TAG = 16;
    private byte[] _data;
    private int _count;

    public static boolean gotResult;
    public static ListMessage result;

    public String getData() {
        return new String(_data, DxramFsConfig.STRING_STD_CHARSET);
    }

    public int getCount() {
        return _count;
    }

    @Override
    protected final int getPayloadLength() {
        return ObjectSizeUtil.sizeofByteArray(_data) + Integer.BYTES;
    }

    @Override
    protected final void writePayload(
            final AbstractMessageExporter p_exporter
    ) {
        p_exporter.writeByteArray(_data);
        p_exporter.writeInt(_count);
    }

    @Override
    protected final void readPayload(
            final AbstractMessageImporter p_importer
    ) {
        _data = p_importer.readByteArray(_data);
        _count = p_importer.readInt(_count);
    }

    // ---------------------------------------------------------------

    public ListMessage() {
        super();
    }

    public ListMessage(final short p_destination) {
        super(p_destination, ListMessage.MTYPE, ListMessage.TAG);
        _data = new byte[DxramFsConfig.max_pathlength_chars];
        _count = 0;
    }

    public ListMessage(final short p_destination, final String p_data, final int p_count) {
        super(p_destination, ListMessage.MTYPE, ListMessage.TAG);
        gotResult = false;
        _data = p_data.getBytes(DxramFsConfig.STRING_STD_CHARSET);
        _count = p_count;
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
    public String[] send(DXNet dxnet) {
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
            if (result.getCount() < 1) { // good till < 0,but < 1 may be better?!
                return null;
            } else {
                return result.getData().split("/");
            }
        } catch (NetworkException e) {
            e.printStackTrace();
            return null;
        }
    }


    public static class InHandler implements MessageReceiver {

        @Override
        public void onIncomingMessage(Message p_message) {
            result = (ListMessage) p_message;
            LOG.info(result.getData());
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
