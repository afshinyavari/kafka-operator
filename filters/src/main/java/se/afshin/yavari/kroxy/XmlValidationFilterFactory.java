package se.afshin.yavari.kroxy;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Plugin(configType = XmlValidationFilterConfig.class)
public class XmlValidationFilterFactory
        implements FilterFactory<XmlValidationFilterConfig, XmlSchemaStore> {

    private static final Logger log = LoggerFactory.getLogger(XmlValidationFilterFactory.class);

    @Override
    public XmlSchemaStore initialize(FilterFactoryContext context, XmlValidationFilterConfig config)
            throws PluginConfigurationException {
        log.info("Initializing XmlValidationFilter — bootstrapServers={}, schemaTopic={}, apiPort={}",
            config.getBootstrapServers(), config.getSchemaTopic(), config.getApiPort());

        XmlSchemaStore store = new XmlSchemaStore(
            config.getBootstrapServers(),
            config.getSchemaTopic(),
            config.getValidationThreadPoolSize()
        );

        try {
            // Blocks until the schema topic high-watermark is consumed — no produce
            // requests are accepted until this returns.
            store.start();

            SchemaManagementApi api = new SchemaManagementApi(store);
            api.start(config.getApiPort());
        } catch (Exception e) {
            store.close();
            throw new PluginConfigurationException("Failed to initialize XmlValidationFilter", e);
        }

        return store;
    }

    @Override
    public Filter createFilter(FilterFactoryContext context, XmlSchemaStore store) {
        return new XmlValidationFilter(store);
    }

    @Override
    public void close(XmlSchemaStore store) {
        try {
            store.close();
        } catch (Exception e) {
            log.warn("Error closing XmlSchemaStore", e);
        }
    }
}
