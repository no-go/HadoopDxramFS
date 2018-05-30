/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.engine;

import java.util.HashMap;
import java.util.Map;

import com.google.gson.annotations.Expose;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.unit.IPV4Unit;

/**
 * Context object with settings for the engine as well as component and service instances
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 20.10.2016
 */
public class DXRAMContext {
    private static final Logger LOGGER = LogManager.getFormatterLogger(DXRAMContext.class.getSimpleName());

    /**
     * Engine specific settings
     */
    @Expose
    private Config m_config = new Config();

    /**
     * List of components
     */
    private final Map<String, AbstractDXRAMComponent> m_components = new HashMap<>();

    /**
     * List of services
     */
    private final Map<String, AbstractDXRAMService> m_services = new HashMap<>();

    /**
     * Constructor
     */
    DXRAMContext() {

    }

    /**
     * Get the configuration
     *
     * @return Configuration
     */
    Config getConfig() {
        return m_config;
    }

    /**
     * Get all component instances
     *
     * @return Component instances
     */
    Map<String, AbstractDXRAMComponent> getComponents() {
        return m_components;
    }

    /**
     * Get all service instances
     *
     * @return Service instances
     */
    Map<String, AbstractDXRAMService> getServices() {
        return m_services;
    }

    /**
     * Run configuration value verification on all component configurations
     *
     * @return True if verification successful, false on failure
     */
    boolean verifyConfigurationValuesComponents() {
        for (AbstractDXRAMComponentConfig config : m_config.m_componentConfigs.values()) {
            // #if LOGGER >= DEBUG
            LOGGER.debug("Verifying component configuration values of %s...", config.getClassName());
            // #endif /* LOGGER >= DEBUG */
            if (!config.verify(m_config)) {
                // #if LOGGER >= ERROR
                LOGGER.error("Verifying component configuration values failed (%s)", config.getClassName());
                // #endif /* LOGGER >= ERROR */
                return false;
            }
        }

        return true;
    }

    /**
     * Run configuration value verification on all service configurations
     *
     * @return True if verification successful, false on failure
     */
    boolean verifyConfigurationValuesServices() {
        for (AbstractDXRAMServiceConfig config : m_config.m_serviceConfigs.values()) {
            // #if LOGGER >= DEBUG
            LOGGER.debug("Verifying service configuration values of %s...", config.getClassName());
            // #endif /* LOGGER >= DEBUG */
            if (!config.verify(m_config)) {
                // #if LOGGER >= ERROR
                LOGGER.error("Verifying service configuration values failed (%s)", config.getClassName());
                // #endif /* LOGGER >= ERROR */
                return false;
            }
        }

        return true;
    }

    /**
     * Create component instances based on the current configuration
     *
     * @param p_manager
     *         Manager to use
     * @param p_nodeRole
     *         Current node role
     */
    void createComponentsFromConfig(final DXRAMComponentManager p_manager, final NodeRole p_nodeRole) {
        m_components.clear();

        for (AbstractDXRAMComponentConfig config : m_config.m_componentConfigs.values()) {
            if (p_nodeRole == NodeRole.SUPERPEER && config.isEnabledForSuperpeer() || p_nodeRole == NodeRole.PEER && config.isEnabledForPeer()) {
                AbstractDXRAMComponent comp = p_manager.createInstance(config.getComponentClass());
                comp.setConfig(config);

                if (p_nodeRole == NodeRole.SUPERPEER && comp.supportsSuperpeer() || p_nodeRole == NodeRole.PEER && comp.supportsPeer()) {
                    m_components.put(comp.getClass().getSimpleName(), comp);
                } else {
                    // #if LOGGER >= ERROR
                    LOGGER.error("Creating instance of component '%s' not possible on current node type '%s', not supported", comp.getComponentName(),
                            p_nodeRole);
                    // #endif /* LOGGER >= ERROR */
                }
            }
        }
    }

    /**
     * Create service instances based on the current configuration
     *
     * @param p_manager
     *         Manager to use
     * @param p_nodeRole
     *         Current node role
     */
    void createServicesFromConfig(final DXRAMServiceManager p_manager, final NodeRole p_nodeRole) {
        m_services.clear();

        for (AbstractDXRAMServiceConfig config : m_config.m_serviceConfigs.values()) {
            if (p_nodeRole == NodeRole.SUPERPEER && config.isEnabledForSuperpeer() || p_nodeRole == NodeRole.PEER && config.isEnabledForPeer()) {
                AbstractDXRAMService serv = p_manager.createInstance(config.getServiceClass());
                serv.setConfig(config);

                if (p_nodeRole == NodeRole.SUPERPEER && serv.supportsSuperpeer() || p_nodeRole == NodeRole.PEER && serv.supportsPeer()) {
                    m_services.put(serv.getClass().getSimpleName(), serv);
                } else {
                    // #if LOGGER >= ERROR
                    LOGGER.error("Creating instance of service '%s' not possible on current node type '%s', not supported", serv.getServiceName(), p_nodeRole);
                    // #endif /* LOGGER >= ERROR */
                }
            }
        }
    }

    /**
     * Fill the context with all components that registered at the DXRAMComponentManager
     *
     * @param p_manager
     *         Manager to use
     */
    void createDefaultComponents(final DXRAMComponentManager p_manager) {
        m_components.clear();
        m_config.m_componentConfigs.clear();

        for (AbstractDXRAMComponent component : p_manager.createAllInstances()) {
            m_components.put(component.getClass().getSimpleName(), component);
            m_config.m_componentConfigs.put(component.getConfig().getClass().getSimpleName(), component.getConfig());
        }
    }

    /**
     * Fill the context with all services that registered at the DXRAMServiceManager
     *
     * @param p_manager
     *         Manager to use
     */
    void createDefaultServices(final DXRAMServiceManager p_manager) {
        m_services.clear();
        m_config.m_serviceConfigs.clear();

        for (AbstractDXRAMService service : p_manager.createAllInstances()) {
            m_services.put(service.getClass().getSimpleName(), service);
            m_config.m_serviceConfigs.put(service.getConfig().getClass().getSimpleName(), service.getConfig());
        }
    }

    /**
     * Class providing configuration values for engine and all components/services
     */
    public static class Config {
        /**
         * Engine specific settings
         */
        @Expose
        private EngineConfig m_engineConfig = new EngineConfig();

        /**
         * Component configurations
         */
        @Expose
        private Map<String, AbstractDXRAMComponentConfig> m_componentConfigs = new HashMap<>();

        /**
         * Service configurations
         */
        @Expose
        private Map<String, AbstractDXRAMServiceConfig> m_serviceConfigs = new HashMap<>();

        /**
         * Get the engine configuration
         */
        public EngineConfig getEngineConfig() {
            return m_engineConfig;
        }

        /**
         * Get the configuration of a specific component
         *
         * @param p_class
         *         Class of the component configuration to get
         * @return Component configuration class
         */
        public <T extends AbstractDXRAMComponentConfig> T getComponentConfig(final Class<T> p_class) {
            AbstractDXRAMComponentConfig conf = m_componentConfigs.get(p_class.getSimpleName());

            return p_class.cast(conf);
        }

        /**
         * Get the configuration of a specific service
         *
         * @param p_class
         *         Class of the service configuration to get
         * @return Service configuration class
         */
        public <T extends AbstractDXRAMServiceConfig> T getServiceConfig(final Class<T> p_class) {
            AbstractDXRAMServiceConfig conf = m_serviceConfigs.get(p_class.getSimpleName());

            return p_class.cast(conf);
        }
    }

    /**
     * Config for the engine
     */
    public static class EngineConfig {
        /**
         * Address and port of this instance
         */
        @Expose
        private IPV4Unit m_address = new IPV4Unit("127.0.0.1", 22222);

        /**
         * Role of this instance (superpeer, peer, terminal)
         */
        @Expose
        private String m_role = "Peer";

        /**
         * Path to jni dependencies
         */
        @Expose
        private String m_jniPath = "jni";

        /**
         * Constructor
         */
        EngineConfig() {

        }

        /**
         * Get the address assigned to the DXRAM instance
         *
         * @return Address
         */
        public IPV4Unit getAddress() {
            return m_address;
        }

        /**
         * Role assigned for this DXRAM instance
         *
         * @return Role
         */
        public NodeRole getRole() {
            return NodeRole.toNodeRole(m_role);
        }

        /**
         * Get the path to the folder with the JNI compiled libraries
         *
         * @return Path to JNI libraries
         */
        String getJNIPath() {
            return m_jniPath;
        }
    }
}
