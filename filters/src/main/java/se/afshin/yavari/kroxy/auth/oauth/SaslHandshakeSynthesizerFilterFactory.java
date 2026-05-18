package se.afshin.yavari.kroxy.auth.oauth;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;

@Plugin(configType = SaslHandshakeSynthesizerConfig.class)
public class SaslHandshakeSynthesizerFilterFactory implements FilterFactory<SaslHandshakeSynthesizerConfig, Void> {

    @Override
    public Void initialize(FilterFactoryContext context, SaslHandshakeSynthesizerConfig config) {
        return null;
    }

    @Override
    public Filter createFilter(FilterFactoryContext context, Void unused) {
        return new SaslHandshakeSynthesizerFilter();
    }
}
