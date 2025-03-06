package io.trino.spooling.local;

import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.trino.filesystem.local.LocalFileSystem;
import io.trino.spi.spool.SpooledLocation;
import io.trino.spi.spool.SpooledLocation.DirectLocation;
import io.trino.spi.spool.SpooledSegmentHandle;
import io.trino.spi.spool.SpoolingContext;
import io.trino.spi.spool.SpoolingManager;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class LocalSpoolingManager
        implements SpoolingManager
{
    private final LocalFileSystem filesystem;

    @Inject
    public LocalSpoolingManager(LocalSpoolingConfig config)
    {
        this.filesystem = new LocalFileSystem(config.getRootPath());
    }

    @Override
    public SpooledSegmentHandle create(SpoolingContext context)
    {
        return null;
    }

    @Override
    public OutputStream createOutputStream(SpooledSegmentHandle handle)
            throws IOException
    {
        return null;
    }

    @Override
    public InputStream openInputStream(SpooledSegmentHandle handle)
            throws IOException
    {
        return null;
    }

    @Override
    public void acknowledge(SpooledSegmentHandle handle)
            throws IOException
    {

    }

    @Override
    public Optional<DirectLocation> directLocation(SpooledSegmentHandle handle)
            throws IOException
    {
        return Optional.empty();
    }

    @Override
    public SpooledLocation location(SpooledSegmentHandle handle)
            throws IOException
    {
        return null;
    }

    @Override
    public SpooledSegmentHandle handle(Slice identifier, Map<String, List<String>> headers)
    {
        return null;
    }
}
