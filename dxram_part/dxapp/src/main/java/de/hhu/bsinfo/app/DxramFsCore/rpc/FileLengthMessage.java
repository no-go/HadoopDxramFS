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

public class FileLengthMessage extends Message {

    public static final Logger LOG = LogManager.getLogger(FileLengthMessage.class.getName());
    public static final byte MTYPE = 42;
    public static final byte TAG = 14;
    private byte[] _data;
    private long _length;

    public static boolean gotResult;
    public static FileLengthMessage result;

    public String get_data() {
        return new String(_data, DxramFsConfig.STRING_STD_CHARSET);
    }

    public long get_length() {
        return _length;
    }

    public void set_length(long _length) {
        this._length = _length;
    }


    @Override
    protected final int getPayloadLength() {
        return Long.BYTES + ObjectSizeUtil.sizeofByteArray(_data);
    }

    @Override
    protected final void writePayload(
            final AbstractMessageExporter p_exporter
    ) {
        p_exporter.writeLong(_length);
        p_exporter.writeByteArray(_data);
    }

    @Override
    protected final void readPayload(
            final AbstractMessageImporter p_importer
    ) {
        _length = p_importer.readLong(_length);
        _data = p_importer.readByteArray(_data);
    }

    // ---------------------------------------------------------------

    public FileLengthMessage() {
        super();
    }

    public FileLengthMessage(final short p_destination) {
        super(p_destination, FileLengthMessage.MTYPE, FileLengthMessage.TAG);
        _data = new byte[DxramFsConfig.max_pathlength_chars];
        _length = -1;
    }

    public FileLengthMessage(final short p_destination, final String p_data) {
        super(p_destination, FileLengthMessage.MTYPE, FileLengthMessage.TAG);
        gotResult = false;
        _data = p_data.getBytes(DxramFsConfig.STRING_STD_CHARSET);
        _length = -1;
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
    public long send(DXNet dxnet) {
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
                return result.get_length();
            } else {
                return -1;
            }
        } catch (NetworkException e) {
            e.printStackTrace();
            return -1;
        }
    }


    public static class InHandler implements MessageReceiver {

        @Override
        public void onIncomingMessage(Message p_message) {
            result = (FileLengthMessage) p_message;
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
