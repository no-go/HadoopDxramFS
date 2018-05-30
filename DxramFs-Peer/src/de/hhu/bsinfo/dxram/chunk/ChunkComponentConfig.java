package de.hhu.bsinfo.dxram.chunk;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponentConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;

/**
 * Config for the ChunkComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class ChunkComponentConfig extends AbstractDXRAMComponentConfig {
    /**
     * Constructor
     */
    public ChunkComponentConfig() {
        super(ChunkComponent.class, false, true);
    }

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
        return true;
    }
}
