package com.docner.hadoop.fuse;

import java.io.FileNotFoundException;
import jnr.ffi.Pointer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import jnr.constants.platform.OpenFlags;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import ru.serce.jnrfuse.struct.FuseFileInfo;

class OpenFile implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(OpenFile.class.getName());
    protected static final int BUFFER_SIZE = 4096;

    private final FileSystem hadoop;
    private final FileStatus before;
    private final FSDataInputStream in;
    private final FSDataOutputStream out;
    private final Set<OpenFlags> flags;
    private final long handle;
    private final AtomicLong sequence = new AtomicLong();

    private OpenFile(FileSystem hadoop, FileStatus before, FSDataInputStream in, FSDataOutputStream out, Set<OpenFlags> flags) {
        this.handle = sequence.getAndIncrement();
        this.hadoop = hadoop;
        this.before = before;
        this.in = in;
        this.flags = flags;
        this.out = out;
    }

    public static OpenFile opening(FileSystem hadoop, final Path path, Set<OpenFlags> flags) throws IOException {
        FileStatus status;
        boolean exists;
        try {
            status = hadoop.getFileStatus(path);
        } catch (FileNotFoundException fnf) {
            if (!flags.contains(OpenFlags.O_WRONLY)) {
                throw fnf;
            }
            status = new FileStatus() {
                @Override
                public Path getPath() {
                    return path;
                }

                @Override
                public boolean isDirectory() {
                    return false;
                }

                @Override
                public boolean isFile() {
                    return false;
                }

                @Override
                public boolean isSymlink() {
                    return false;
                }
            };
        }
        exists = status != null && (status.isFile() || status.isDirectory() || status.isSymlink());

        FSDataOutputStream out = null;
        FSDataInputStream in = null;
        if (flags.contains(OpenFlags.O_WRONLY)) {

            if (exists && flags.contains(OpenFlags.O_APPEND)) {
                LOG.log(Level.INFO, "Appending to {0}", path);
                out = hadoop.append(path);
            } else if (exists && flags.contains(OpenFlags.O_EXCL) && flags.contains(OpenFlags.O_CREAT)) {
                throw new FileAlreadyExistsException(path.toString());
            } else {
                out = hadoop.create(path, true);
            }
        } else if (!flags.contains(OpenFlags.O_CREAT)) {
            if (flags.contains(OpenFlags.O_RDWR)) {
                LOG.log(Level.WARNING, "Treating O_RDWR as READ ONLY: {0}", path.toUri().toASCIIString());
            }
            if (!exists) {
                throw new FileNotFoundException(path.toUri().toASCIIString());
            }
            if (status.isFile()) {
                in = hadoop.open(path);
            } else if (status.isSymlink()) {
                return opening(hadoop, status.getSymlink(), flags);
            } else {
                throw new IOException("Cannot open a directory");
            }
        }
        return new OpenFile(hadoop, status, in, out, flags);
    }

    /**
     * Reads up to {@code size} bytes beginning at {@code offset} into
     * {@code buf}.
     *
     * @param buf Buffer
     * @param offset Position of first byte to read
     * @param size Number of bytes to read
     * @return either containing the actual number of bytes read (can be less
     * than {@code size} if reached EOF) or failing with an {@link IOException}
     */
    public int read(Pointer buf, long offset, long size) throws IOException {
        ByteBuffer bb = ByteBuffer.allocate(BUFFER_SIZE);
        long pos = offset;
        while (pos < offset + size) {
            int read = in.read(bb.array());
            if (read == -1) {
                break;
            }
            int n = (int) Math.min(offset + size - pos, read); //result know to return <= 1024
            buf.put(pos - offset, bb.array(), 0, n);
            pos += n;
        }
        int totalRead = (int) (pos - offset);
        return totalRead;
    }

    public int write(Pointer buf, long offset, long size) throws IOException {
        ByteBuffer bb = ByteBuffer.allocate(BUFFER_SIZE);
        long written = 0;
        do {
            long remaining = size - written;
            bb.clear();
            int len = (int) Math.min(remaining, bb.capacity());
            buf.get(written, bb.array(), 0, len);
            bb.limit(len);

            out.write(bb.array(), 0, len);

            written += len;
        } while (written < size);

        /*if (written > 0) {
            out.flush();
        }*/
        return (int) written; // TODO wtf cast    }
    }

    public int flush(FuseFileInfo fi) throws IOException {
        LOG.info("FLUSH");
        if (out != null) {
            out.hflush();
        }
        return 0;
    }

    public int sync(int datasync, FuseFileInfo fi) throws IOException {
        if (out != null) {
            out.hsync();
        }
        return 0;
    }

    public FSDataInputStream getDataIn() {
        return in;
    }

    public FSDataOutputStream getDataOut() {
        return out;
    }

    public boolean isReading() {
        return in != null;
    }

    public boolean isWriting() {
        return out != null;
    }

    public boolean isEmpty() {
        if (isReading()) {
            return in == null;
        } else if (isWriting()) {
            return out == null;
        } else {
            return false;
        }
    }

    public Set<OpenFlags> getFlags() {
        return flags;
    }

    public FileStatus getBefore() {
        return before;
    }

    public Path getPath() {
        return before == null ? null : before.getPath();
    }

    public FileSystem getHadoop() {
        return hadoop;
    }

    public long getHandle() {
        return handle;
    }

    @Override
    public String toString() {
        return "@" + OpenFile.class.getName() + "|handle=" + handle + "|uri=" + uri();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof OpenFile && hashCode() == obj.hashCode();
    }

    private String uri() {
        return this.before == null || this.before.getPath() == null ? null : this.before.getPath().toUri().toASCIIString();
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 71 * hash + Objects.hashCode(uri());
        hash = 71 * hash + (int) (this.handle ^ (this.handle >>> 32));
        return hash;
    }

    @Override
    public void close() throws IOException {
        if (in != null) {
            in.close();
        }
        if (out != null) {
            out.close();
        }
    }

}
