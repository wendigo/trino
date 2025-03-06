package io.trino.spooling.local;

import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;

public class LocalSpoolingModule
        extends AbstractConfigurationAwareModule
{
    private final boolean coordinator;

    public LocalSpoolingModule(boolean coordinator)
    {
        this.coordinator = coordinator;
    }

    @Override
    protected void setup(Binder binder)
    {
    }
}
