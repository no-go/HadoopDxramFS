package de.hhu.bsinfo.dxram.engine;

/**
 * Config for NullComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class NullComponentConfig extends AbstractDXRAMComponentConfig {
    /**
     * Constructor
     */
    public NullComponentConfig() {
        super(NullComponent.class, true, true);
    }

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
        return true;
    }
}
