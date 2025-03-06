package io.trino.spooling.local;

import io.trino.spi.Plugin;
import io.trino.spi.spool.SpoolingManagerFactory;

import java.util.List;

public class LocalSpoolingPlugin
        implements Plugin
{
    @Override
    public Iterable<SpoolingManagerFactory> getSpoolingManagerFactories()
    {
        return List.of(new LocalSpoolingManagerFactory());
    }
}
