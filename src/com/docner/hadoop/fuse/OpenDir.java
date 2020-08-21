package com.docner.hadoop.fuse;

import java.io.IOException;
import java.util.Objects;
import jnr.ffi.Pointer;
import ru.serce.jnrfuse.FuseFillDir;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RemoteIterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.struct.FileStat;

class OpenDir {

    private static final Logger LOG = Logger.getLogger(OpenDir.class.getName());
    private final FileSystem hadoop;
    private final FileStatus before;
    private final RemoteIterator<FileStatus> children;
    private final long handle;
    private final AtomicLong sequence = new AtomicLong();

    private int alreadyDone = 0;

    public OpenDir(FileSystem provider, FileStatus before) throws IOException {
        this.handle = sequence.getAndIncrement();
        this.hadoop = provider;
        this.before = before;

        children = hadoop.listStatusIterator(before.getPath());
        LOG.log(Level.INFO, "Opened directory {0} with at least one file: {1}", new Object[]{before.getPath(), children.hasNext()});
    }

    // https://www.cs.hmc.edu/~geoff/classes/hmc.cs135.201001/homework/fuse/fuse_doc.html#readdir-details
    public int list(Pointer buf, FuseFillDir filler, int offset) {
        LOG.log(Level.INFO, "Dir Listing children from offset {0} already done {1}", new Object[]{offset, alreadyDone});
        try {
            while (children.hasNext()) {
                FileStatus child = children.next();
                String childName = child.getPath().getName();
                LOG.log(Level.INFO, "Dir Listing child {0} for {1}", new Object[]{childName, alreadyDone});

                FileStat stat = new FileStat(buf.getRuntime());
                Attributes.copy(child, stat);
                
                int nextOffset = children.hasNext() ? alreadyDone + 1 : 0;
                int result = filler.apply(buf, childName, stat, nextOffset);
                alreadyDone++;

                if (result != 0) {
                    return 0;
                }
            }
            return 0;
        } catch (IOException | RuntimeException e) {
            LOG.log(Level.WARNING, "getattr failed.", e);
            return -ErrorCodes.EIO();
        }
    }

    public boolean isEmpty() throws IOException {
        return children == null || (!children.hasNext());
    }

    @Override
    public String toString() {
        return "@" + OpenDir.class.getName() + "|done=" + alreadyDone;
    }

    public long getHandle() {
        return handle;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof OpenDir && hashCode() == obj.hashCode();
    }

    private String uri() {
        return this.before == null || this.before.getPath() == null ? null : this.before.getPath().toUri().toASCIIString();
    }

    @Override
    public int hashCode() {
        int hash = 28;
        hash = 73 * hash + Objects.hashCode(uri());
        hash = 73 * hash + (int) (this.handle ^ (this.handle >>> 32));
        return hash;
    }
}
