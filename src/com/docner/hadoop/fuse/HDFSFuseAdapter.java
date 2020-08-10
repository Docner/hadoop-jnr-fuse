package com.docner.hadoop.fuse;

import java.io.FileNotFoundException;
import jnr.ffi.Pointer;
import jnr.ffi.types.off_t;
import jnr.ffi.types.size_t;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;
import ru.serce.jnrfuse.struct.Statvfs;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import jnr.constants.platform.OpenFlags;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import static ru.serce.jnrfuse.flags.AccessConstants.R_OK;
import static ru.serce.jnrfuse.flags.AccessConstants.W_OK;
import static ru.serce.jnrfuse.flags.AccessConstants.X_OK;
import ru.serce.jnrfuse.flags.XAttrConstants;

/**
 * FUSE-HDFSFuseAdapter for HDFS based on Sergey Tselovalnikov's
 <a href="https://github.com/SerCeMan/jnr-fuse/blob/0.5.1/src/main/java/ru/serce/jnrfuse/examples/HelloFuse.java">HelloFuse</a>
 * <a href="https://www.cs.hmc.edu/~geoff/classes/hmc.cs135.201001/homework/fuse/fuse_doc.html">Fuse
 * howto</a>
 */
public class HDFSFuseAdapter extends FuseStubFS implements AutoCloseable {

    private static final Logger LOG = java.util.logging.Logger.getLogger(HDFSFuseAdapter.class.getName());
    private static final int BLOCKSIZE = 4096;
    protected final Path root;
    private final int maxFileNameLength;
    protected final Configuration configuration;
    protected final UserGroupInformation login;
    protected final FileSystem hdfs;
    private OpenDirectories directories;
    private OpenFiles files;
    private boolean blockXattrs = false;

    public HDFSFuseAdapter(Path root, int maxFileNameLength, FileSystem fileStore, Configuration config, UserGroupInformation login) throws IOException {
        this.root = root;
        this.maxFileNameLength = maxFileNameLength;
        this.hdfs = fileStore;
        this.login = login;
        this.configuration = config;
    }

    protected Path resolveParentPath(String absolutePath) throws IOException {
        String parent; // somehow it does not add the home directory to it. 
        String name;
        int pos = absolutePath.lastIndexOf('/');
        if (pos < 1) {
            parent = "/";
            name = absolutePath;
        } else {
            parent = absolutePath.substring(0, pos);
            name = absolutePath.substring(pos + 1);
        }
        Path resolved = resolvePath(parent);

        while (name.charAt(0) == '/') {
            name = name.substring(1);
        }
        return new Path(resolved, name);
    }

    protected Path resolvePath(String absolutePath) throws IOException {
        String relativePath = absolutePath;
        /*while (relativePath.length()>0 && relativePath.charAt(0) == '/') {
            relativePath = relativePath.substring(1);
        }*/
        Path resolved;
        if ("/".equals(relativePath) || relativePath.isBlank()) {
            resolved = root;
        } else {
            while (relativePath.length() > 0 && relativePath.charAt(0) == '/') {
                relativePath = relativePath.substring(1);
            }
            resolved = hdfs.resolvePath(new Path(root, relativePath));
        }
        LOG.log(Level.FINER, "RESOLVED = {0}", resolved.toString());
        return resolved;
    }

    @Override
    public int statfs(String path, Statvfs stbuf) {
        try {
            long total = hdfs.getStatus().getCapacity();
            long avail = hdfs.getStatus().getRemaining();
            long tBlocks = total / BLOCKSIZE;
            long aBlocks = avail / BLOCKSIZE;
            stbuf.f_bsize.set(BLOCKSIZE);
            stbuf.f_frsize.set(BLOCKSIZE);
            stbuf.f_blocks.set(tBlocks);
            stbuf.f_bavail.set(aBlocks);
            stbuf.f_bfree.set(aBlocks);
            stbuf.f_namemax.set(maxFileNameLength);
            LOG.log(Level.INFO, "statfs {0} ({1} / {1})", new Object[]{path, avail, total});
            return 0;
        } catch (IOException | RuntimeException e) {
            LOG.log(Level.WARNING, "statfs " + path + " failed.", e);
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public int getattr(String path, FileStat stat) {
        try {
            Path node = resolvePath(path);

            FileStatus fileStatus = hdfs.getFileStatus(node);
            Attributes.copy(fileStatus, stat);
            return 0;
        } catch (FileNotFoundException fnf) {
            LOG.log(Level.INFO, "no file: {0}", fnf.getMessage());
            return -ErrorCodes.ENOENT();

        } catch (IOException | RuntimeException e) {
            LOG.log(Level.WARNING, "statfs " + path + " failed.", e);
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public int access(String path, int mask) {
        try {
            Path node = resolvePath(path);

            try {
                if ((mask & R_OK) > 0) {
                    hdfs.access(node, FsAction.READ);
                }
                if ((mask & W_OK) > 0) {
                    hdfs.access(node, FsAction.WRITE);
                }
                if ((mask & X_OK) > 0) {
                    hdfs.access(node, FsAction.EXECUTE);
                }
                return 0;
            } catch (FileNotFoundException fnf) {
                return -ErrorCodes.ENOENT();
            } catch (AccessControlException ill) {
                return -ErrorCodes.EACCES();
            }
        } catch (IOException | RuntimeException e) {
            LOG.log(Level.WARNING, "statfs " + path + " failed.", e);
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public int opendir(String path, FuseFileInfo fi) {
        try {
            Path node = resolvePath(path);

            FileStatus status;
            try {
                status = hdfs.getFileStatus(node);
            } catch (FileNotFoundException fnf) {
                return -ErrorCodes.ENOENT();
            }
            if (status.isDirectory()) {
                long handle = directories.open(status);
                fi.fh.set(handle);

                return 0;
            } else {
                return -ErrorCodes.ENOTDIR();
            }
        } catch (IOException | RuntimeException e) {
            LOG.log(Level.WARNING, "statfs " + path + " failed.", e);
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public int readdir(String path, Pointer buf, FuseFillDir filler, @off_t long offset, FuseFileInfo fi) {
        if (offset > Integer.MAX_VALUE) {
            LOG.log(Level.WARNING, "readdir() only supported for up to 2^31 entries, but attempted to read from offset {}", offset);
            return -ErrorCodes.EOVERFLOW();
        }
        try {
            OpenDir openDir = directories.get(fi.fh.get());
            if (openDir == null || openDir.isEmpty()) {
                return -ErrorCodes.EBADF();
            }

            return openDir.list(buf, filler, (int) offset);

        } catch (IOException | RuntimeException e) {
            LOG.log(Level.WARNING, "statfs " + path + " failed.", e);
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public int releasedir(String path, FuseFileInfo fi) {
        long handle = fi.fh.get();
        LOG.log(Level.INFO, "Closing dir handle {0}", handle);
        directories.close(handle);
        return 0;
    }

    @Override
    public int create(String rawPath, long mode, FuseFileInfo fi) {
        try {
            Set<OpenFlags> flags = new LinkedHashSet(BitMaskEnumUtil.bitMaskToSet(OpenFlags.class, fi.flags.longValue()));
            Path target = resolveParentPath(rawPath);

            long handle = files.open(target, flags);
            fi.fh.set(handle);
            return 0;

        } catch (IOException | RuntimeException ioe) {
            LOG.log(Level.WARNING, "create " + rawPath + " failed.", ioe);
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public int open(String path, FuseFileInfo fi) {
        try {
            Path node = resolvePath(path);

            FileStatus status;
            try {
                status = hdfs.getFileStatus(node);
            } catch (FileNotFoundException fnf) {
                return -ErrorCodes.ENOENT();
            }

            if (status.isFile()) {
                long handle = files.open(node, BitMaskEnumUtil.bitMaskToSet(OpenFlags.class, fi.flags.longValue()));
                fi.fh.set(handle);
                return 0;
            } else if (status.isDirectory()) {
                return -ErrorCodes.EISDIR();
            } else {
                return -ErrorCodes.EIO(); //TODO: correct?
            }

        } catch (IOException | RuntimeException e) {
            LOG.log(Level.WARNING, "open " + path + " failed.", e);
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public int read(String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {
        OpenFile openFile = files.get(fi.fh.get());

        if (openFile == null || openFile.isEmpty()) {
            return -ErrorCodes.EBADF();
        }
        try {
            return openFile.read(buf, offset, size);
        } catch (IOException | RuntimeException ioe) {
            LOG.log(Level.WARNING, "read " + path + " failed.", ioe);
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public int write(String path, Pointer buf, long size, long offset, FuseFileInfo fi) {
        OpenFile openFile = files.get(fi.fh.get());

        if (openFile == null) {
            return -ErrorCodes.EBADF();
        }
        try {
            return openFile.write(buf, offset, size);
        } catch (IOException | RuntimeException ioe) {
            LOG.log(Level.WARNING, "write " + path + " failed.", ioe);
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public int flush(String path, FuseFileInfo fi) {
        OpenFile openFile = files.get(fi.fh.get());

        if (openFile == null) {
            return -ErrorCodes.EBADF();
        }
        try {
            return openFile.flush(fi);
        } catch (IOException | RuntimeException ioe) {
            LOG.log(Level.WARNING, "flush " + path + " failed.", ioe);
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public int fsync(String path, int isdatasync, FuseFileInfo fi) {
        OpenFile openFile = files.get(fi.fh.get());

        if (openFile == null) {
            return -ErrorCodes.EBADF();
        }
        try {
            return openFile.sync(isdatasync, fi);
        } catch (IOException | RuntimeException ioe) {
            LOG.log(Level.WARNING, "fsync " + path + " failed.", ioe);
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public int release(String path, FuseFileInfo fi) {
        try {
            files.close(fi.fh.get());
            return 0;
        } catch (IOException | RuntimeException ioe) {
            LOG.log(Level.WARNING, "release " + path + " failed.", ioe);
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public int readlink(String path, Pointer buf, long size) {
        try {
            Path node = resolvePath(path);

            FileStatus status;
            try {
                status = hdfs.getFileStatus(node);
            } catch (FileNotFoundException fnf) {
                return -ErrorCodes.ENOENT();
            }
            if (status.isSymlink()) {
                Path target = status.getSymlink();
                String name = target.getName();

                int maxSize = size == 0 ? 0 : (int) size - 1;
                buf.putString(0, name, maxSize, StandardCharsets.UTF_8);
                buf.putByte(maxSize, (byte) 0x00);
                return 0;
            }

            return 0;
        } catch (IOException | RuntimeException ioe) {
            LOG.log(Level.WARNING, "readlink " + path + " failed.", ioe);
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public int listxattr(String path, Pointer buf, long size) {
        try {
            Path node = resolvePath(path);

            FileStatus status;
            try {
                status = hdfs.getFileStatus(node);
            } catch (FileNotFoundException fnf) {
                return -ErrorCodes.ENOENT();
            }

            Map<String, byte[]> xattrs = hdfs.getXAttrs(node);
            if (xattrs != null) {

                long written = 0;
                for (String name : new TreeSet<>(xattrs.keySet())) {

                    byte[] encoded = name.getBytes(StandardCharsets.UTF_8);
                    int len = encoded.length;
                    if ((written + len) > size) {
                        return -ErrorCodes.ERANGE();
                    }
                    buf.put(written, encoded, 0, len);
                    written += len;
                }
            }
            return 0;

        } catch (UnsupportedOperationException uns) {
            return -ErrorCodes.ENOTSUP();

        } catch (IOException | RuntimeException ioe) {
            LOG.log(Level.WARNING, "listxattr " + path + " failed.", ioe);
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public int getxattr(String path, String name, Pointer buf, long size) {
        if (blockXattrs) {
            return 0;
        }
        try {
            Path node = resolvePath(path);

            FileStatus status;
            try {
                status = hdfs.getFileStatus(node);
            } catch (FileNotFoundException fnf) {
                return -ErrorCodes.ENOENT();
            }

            byte[] result = hdfs.getXAttr(node, name);
            if (result == null) {
                return -ErrorCodes.ENOATTR();
            }
            if (result.length > size) {
                return -ErrorCodes.E2BIG();
            }
            buf.put(0, result, 0, result.length);
            return 0;

        } catch (UnsupportedOperationException uns) {
            this.blockXattrs = true;
            return -ErrorCodes.ENOTSUP();
        } catch (org.apache.hadoop.security.AccessControlException access) {
            if (access.getMessage().contains("doesn't have permission for xattr")) {
                blockXattrs = true;
                return 0;//-ErrorCodes.EACCES();
            }
            this.blockXattrs = true;
            return -ErrorCodes.ENOTSUP();

        } catch (IOException | RuntimeException ioe) {
            LOG.log(Level.WARNING, "getxattr " + path + " failed.", ioe);
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public int setxattr(String path, String name, Pointer buf, long size, int flags) {
        try {
            Path node = resolvePath(path);

            FileStatus status;
            try {
                status = hdfs.getFileStatus(node);
            } catch (FileNotFoundException fnf) {
                return -ErrorCodes.ENOENT();
            }
            EnumSet<XAttrSetFlag> flagset = EnumSet.noneOf(XAttrSetFlag.class);
            if ((flags & XAttrConstants.XATTR_CREATE) > 0) {
                flagset.add(XAttrSetFlag.CREATE);
            }
            if ((flags & XAttrConstants.XATTR_REPLACE) > 0) {
                flagset.add(XAttrSetFlag.REPLACE);
            }

            byte[] value = new byte[(int) size];
            buf.get(0, value, 0, (int) size);
            hdfs.setXAttr(node, name, value, flagset);
            return 0;

        } catch (UnsupportedOperationException uns) {
            return -ErrorCodes.ENOTSUP();

        } catch (IOException | RuntimeException ioe) {
            LOG.log(Level.WARNING, "setxattr " + path + " failed.", ioe);
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public int rename(String oldpath, String newpath) {
        //TODO: do we have to check if one of these things are already opened in this filesystem?
        //TODO: What should be default if the source already exists?

        try {
            Path old = resolvePath(oldpath);
            Path target = resolveParentPath(newpath);

            hdfs.rename(old, target);
            return 0;
        } catch (IOException | RuntimeException ioe) {
            LOG.log(Level.WARNING, "rename " + oldpath + " to " + newpath + " failed.", ioe);
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public int mkdir(String path, long mode) {
        try {
            Path target = resolveParentPath(path);
            FsPermission permissions = new FsPermission((short) (mode & 0xfff));

            boolean created = hdfs.mkdirs(target, permissions);
            if (created) {
                return 0;
            }
            else{
                return -ErrorCodes.EIO();
            }
        } catch (IOException | RuntimeException ioe) {
            LOG.log(Level.WARNING, "mkdir " + path + " failed.", ioe);
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public int rmdir(String path) {
        try {
            Path target = resolvePath(path);

            FileStatus status;
            try {
                status = hdfs.getFileStatus(target);
            } catch (FileNotFoundException fnf) {
                return -ErrorCodes.ENOENT();
            }
            if (!status.isDirectory()) {
                return -ErrorCodes.ENOTDIR();
            }
            RemoteIterator<FileStatus> children = hdfs.listStatusIterator(target);
            if (children.hasNext()) {
                return -ErrorCodes.ENOTEMPTY();
            }
            boolean deleted = hdfs.delete(target, false);
            if (deleted) {
                return 0;
            } else {
                return -ErrorCodes.EIO();
            }
        } catch (IOException | RuntimeException ioe) {
            LOG.log(Level.WARNING, "rmdir " + path + " failed.", ioe);
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public int unlink(String path) {
        try {
            Path target = resolvePath(path);

            hdfs.delete(target, true);
            return 0;

        } catch (FileNotFoundException fnf) {
            return -ErrorCodes.ENOENT();
        } catch (IOException | RuntimeException ioe) {
            LOG.log(Level.WARNING, "unlink " + path + " failed.", ioe);
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public Pointer init(Pointer conn) {
        Pointer initialized = super.init(conn); //To change body of generated methods, choose Tools | Templates.

        this.directories = new OpenDirectories(hdfs);
        this.files = new OpenFiles(hdfs);

        return initialized;
    }

    @Override
    public void destroy(Pointer initResult) {
        try {
            close();
        } catch (IOException | RuntimeException e) {
            LOG.log(Level.WARNING, "destroy failed.", e);
        }
    }

    public boolean isMounted() {
        return mounted.get();
    }

    /*
	 * We overwrite the default implementation to skip the "internal" unmount command, because we want to use system commands instead.
	 * See also: https://github.com/cryptomator/fuse-nio-adapter/issues/29
     */
 /*@Override
    public void umount() {
        // this might be called multiple times: explicitly _and_ via a shutdown hook registered during mount() in AbstractFuseFS
        if (mounted.compareAndSet(true, false)) {
            LOG.debug("Marked file system adapter as unmounted.");
        } else {
            LOG.trace("File system adapter already unmounted.");
        }
    }*/
    @Override
    public void close() throws IOException {
        directories.close();
        files.close();
    }
}
