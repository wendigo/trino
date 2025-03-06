package io.trino.spooling.local;

import io.airlift.configuration.validation.FileExists;

import java.nio.file.Path;

public class LocalSpoolingConfig
{
    private Path rootPath;

    @FileExists
    public Path getRootPath()
    {
        return rootPath;
    }

    public LocalSpoolingConfig setRootPath(Path rootPath)
    {
        this.rootPath = rootPath;
        return this;
    }
}
