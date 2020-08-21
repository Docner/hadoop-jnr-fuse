package com.docner.hadoop.fuse;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

class OpenDirectories {

    private static final Logger LOG = Logger.getLogger(OpenDirectories.class.getName());

    private final ConcurrentMap<Long, OpenDir> openDirs = new ConcurrentHashMap<>();
    private final AtomicLong fileHandleGen = new AtomicLong();
    private final FileSystem hadoop;

    public OpenDirectories(FileSystem provider) {
        this.hadoop = provider;
    }

    /**
     * @param path path of the dir to open
     * @return file handle used to identify and close open files.
     */
    public long open(FileStatus dirStatus) throws IOException {

        OpenDir dir = new OpenDir(hadoop, dirStatus);
        long handle = dir.getHandle();

        openDirs.put(handle, dir);
        LOG.log(Level.INFO, "Opening dir {0} {1}", new Object[]{dir.getHandle(), dir});
        return handle;
    }

    public OpenDir get(long dirHandle) {
        return openDirs.get(dirHandle);
    }

    /**
     * Closes the channel identified by the given fileHandle
     *
     * @param fileHandle file handle used to identify
     */
    public void close(long fileHandle) {
        OpenDir dir = openDirs.remove(fileHandle);
        if (dir != null) {
            LOG.log(Level.INFO, "Releasing dir {0} {1}", new Object[]{fileHandle, dir});
        }
    }

    public void close() throws IOException {
        new LinkedHashSet<>(openDirs.keySet()).forEach((handle) -> {
            close(handle);
        });
    }
}
