package de.hhu.bsinfo.dxramfs.core;

import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;

public class A100bMessage extends Message {
    public static final Logger LOG = LogManager.getLogger(A100bMessage.class.getName());
    public static final byte MTYPE = 1;
    public static final byte TAG = 42;
    private byte[] data;

    // @todo mutex oder so
    public static boolean gotMsg;
    public static A100bMessage msg;

    public A100bMessage() {
        super();
    }

    public A100bMessage(final short p_destination) {
        super(p_destination, A100bMessage.MTYPE, A100bMessage.TAG);
        data = new byte[100];
    }

    public A100bMessage(final short p_destination, final String p_data) {
        super(p_destination, A100bMessage.MTYPE, A100bMessage.TAG);
        gotMsg = false;
        data = p_data.getBytes(StandardCharsets.UTF_8);
    }

    public String getData() {
        return new String(data, StandardCharsets.UTF_8);
    }

    @Override
    protected final int getPayloadLength() {
        return ObjectSizeUtil.sizeofByteArray(data);
    }

    @Override
    protected final void writePayload(
            final AbstractMessageExporter p_exporter
    ) {
        p_exporter.writeByteArray(data);
    }

    @Override
    protected final void readPayload(
            final AbstractMessageImporter p_importer
    ) {
        data = p_importer.readByteArray(data);
    }

    public static class InHandler implements MessageReceiver {
        private int _counter = 0;

        @Override
        public void onIncomingMessage(Message p_message) {
            msg = (A100bMessage) p_message;
            LOG.info(msg.getData());
            gotMsg = true;
            _counter++;
        }

        public int getCounter() {
            return _counter;
        }

        public Message lastMsg() {
            return msg;
        }

        public boolean gotMsg() {
            return gotMsg;
        }
    }
}
