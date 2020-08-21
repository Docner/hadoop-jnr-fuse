package com.docner.hadoop.fuse;

import java.io.IOException;
import java.util.LinkedHashSet;
import jnr.constants.platform.OpenFlags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

class OpenFiles {

    private static final Logger LOG = LoggerFactory.getLogger(OpenFiles.class);

    private final ConcurrentMap<Long, OpenFile> openFiles = new ConcurrentHashMap<>();
    private final FileSystem hadoop;

    OpenFiles(FileSystem provider) {
        this.hadoop = provider;
    }

    /**
     * @param path path of the file to open
     * @param flags file open options
     * @return file handle used to identify and close open files.
     */
    public long open(Path path, Set<OpenFlags> flags) throws IOException {

        OpenFile file = OpenFile.opening(hadoop, path, flags);
        openFiles.put(file.getHandle(), file);
        LOG.trace("Opening file {} {}", file.getHandle(), file);
        return file.getHandle();
    }

    public OpenFile get(long fileHandle) {
        return openFiles.get(fileHandle);
    }

    /**
     * Closes the channel identified by the given fileHandle
     *
     * @param fileHandle file handle used to identify
     */
    public void close(long fileHandle) throws IOException {
        OpenFile file = openFiles.remove(fileHandle);
        if (file != null) {
            LOG.trace("Releasing file {} {}", fileHandle, file);
            file.close();
        } else {
            LOG.trace("No open file with handle {} found.", fileHandle);
        }
    }

    public void close() throws IOException {
        for (long handle : new LinkedHashSet<>(openFiles.keySet())) {
            close(handle);
        }
    }
}
