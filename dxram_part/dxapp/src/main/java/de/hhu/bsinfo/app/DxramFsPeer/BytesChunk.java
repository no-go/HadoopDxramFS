package de.hhu.bsinfo.app.dxramfspeer;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public final class BytesChunk extends AbstractChunk {
    private byte[] m_data;
    
        public BytesChunk(final int p_bufferSize) {
        super();
        m_data = new byte[p_bufferSize];
    }

    public BytesChunk(final long p_id, final int p_bufferSize) {
        super(p_id);
        m_data = new byte[p_bufferSize];
    }

    public BytesChunk(final byte[] p_buffer) {
        super();
        m_data = p_buffer;
    }

    public BytesChunk(final long p_id, final byte[] p_buffer) {
        super(p_id);
        m_data = p_buffer;
    }

    public byte[] getData() {
        return m_data;
    }

    public int getSize() {
        return m_data.length;
    }

    @Override
    public void importObject(final Importer p_importer) {
        p_importer.readBytes(m_data);
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeBytes(m_data);
    }

    @Override
    public int sizeofObject() {
        return m_data.length;
    }
}