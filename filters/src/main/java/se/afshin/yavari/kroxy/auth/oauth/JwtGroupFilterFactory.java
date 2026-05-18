package se.afshin.yavari.kroxy.auth.oauth;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;

@Plugin(configType = JwtGroupFilterConfig.class)
public class JwtGroupFilterFactory implements FilterFactory<JwtGroupFilterConfig, String> {

    @Override
    public String initialize(FilterFactoryContext context, JwtGroupFilterConfig config) {
        return config.getGroupsClaim();
    }

    @Override
    public Filter createFilter(FilterFactoryContext context, String groupsClaim) {
        return new JwtGroupFilter(groupsClaim);
    }
}
