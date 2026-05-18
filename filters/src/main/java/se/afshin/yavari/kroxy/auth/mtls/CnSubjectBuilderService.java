package se.afshin.yavari.kroxy.auth.mtls;

import io.kroxylicious.proxy.authentication.TransportSubjectBuilder;
import io.kroxylicious.proxy.authentication.TransportSubjectBuilderService;
import io.kroxylicious.proxy.plugin.Plugin;

@Plugin(configType = CnSubjectBuilderConfig.class)
public class CnSubjectBuilderService implements TransportSubjectBuilderService<CnSubjectBuilderConfig> {

    @Override
    public void initialize(CnSubjectBuilderConfig config) {}

    @Override
    public TransportSubjectBuilder build() {
        return new CnSubjectBuilder();
    }
}
