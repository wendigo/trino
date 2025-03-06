package io.trino.spooling.local;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.spi.spool.SpoolingManager;
import io.trino.spi.spool.SpoolingManagerContext;
import io.trino.spi.spool.SpoolingManagerFactory;
import org.weakref.jmx.guice.MBeanModule;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class LocalSpoolingManagerFactory
        implements SpoolingManagerFactory
{
    @Override
    public String getName()
    {
        return "local";
    }

    @Override
    public SpoolingManager create(Map<String, String> config, SpoolingManagerContext context)
    {
        requireNonNull(config, "requiredConfig is null");
        Bootstrap app = new Bootstrap(
                new LocalSpoolingModule(context.isCoordinator()),
                new MBeanModule(),
                new MBeanServerModule(),
                binder -> {
                    binder.bind(SpoolingManagerContext.class).toInstance(context);
                    binder.bind(OpenTelemetry.class).toInstance(context.getOpenTelemetry());
                });

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize();

        return injector.getInstance(SpoolingManager.class);
    }
}
