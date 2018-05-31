package de.hhu.bsinfo.dxram.engine;

import com.google.gson.annotations.Expose;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Provides configuration values for a component. Use this as a base class for all components to add further configuration values
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public abstract class AbstractDXRAMComponentConfig {
    protected final Logger LOGGER;

    @Expose
    private String m_class;

    @Expose
    private String m_componentClass;

    @Expose
    private boolean m_enabledForSuperpeer;

    @Expose
    private boolean m_enabledForPeer;

    /**
     * Constructor
     *
     * @param p_class
     *         Class extending the abstract component class of this configuration
     * @param p_enabledForSuperpeer
     *         True to enable the component if the node is a superpeer, false to disable
     * @param p_enabledForPeer
     *         True to enable the component if the node is a peer, false to disable
     */
    protected AbstractDXRAMComponentConfig(final Class<? extends AbstractDXRAMComponent> p_class, final boolean p_enabledForSuperpeer, final boolean p_enabledForPeer) {
        LOGGER = LogManager.getFormatterLogger(getClass().getSimpleName());
        m_class = getClass().getName();
        m_componentClass = p_class.getSimpleName();
        m_enabledForSuperpeer = p_enabledForSuperpeer;
        m_enabledForPeer = p_enabledForPeer;
    }

    /**
     * Get the fully qualified class name of the config class
     */
    public String getClassName() {
        return m_class;
    }

    /**
     * Get the fully qualified class name of the component of this configuration
     */
    public String getComponentClass() {
        return m_componentClass;
    }

    /**
     * True to enable the component if the node is a superpeer, false to disable
     */
    public boolean isEnabledForSuperpeer() {
        return m_enabledForSuperpeer;
    }

    /**
     * True to enable the component if the node is a peer, false to disable
     */
    public boolean isEnabledForPeer() {
        return m_enabledForPeer;
    }

    /**
     * Verify the configuration values: Check limits, validate strings, ...
     *
     * @param p_config
     *         Full configuration to access other config values on dependencies
     * @return True if verifcation successful, false on error
     */
    protected abstract boolean verify(final DXRAMContext.Config p_config);
}
