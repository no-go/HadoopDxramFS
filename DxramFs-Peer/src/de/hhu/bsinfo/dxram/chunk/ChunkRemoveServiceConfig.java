package de.hhu.bsinfo.dxram.chunk;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMServiceConfig;

/**
 * Config for the ChunkRemoveService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class ChunkRemoveServiceConfig extends AbstractDXRAMServiceConfig {
    @Expose
    private int m_removerQueueSize = 100000;

    /**
     * Constructor
     */
    public ChunkRemoveServiceConfig() {
        super(ChunkRemoveService.class, false, true);
    }

    /**
     * Size of the queue that stores the remove requests to be processed asynchronously
     */
    public int getRemoverQueueSize() {
        return m_removerQueueSize;
    }

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
        if (m_removerQueueSize < 1) {
            // #if LOGGER >= ERROR
            LOGGER.error("Invalid value (%d) for m_removerQueueSize", m_removerQueueSize);
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        return true;
    }
}
