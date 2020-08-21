package com.docner.hadoop.fuse;

import java.time.Instant;
import java.util.EnumSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import jnr.constants.platform.OpenFlags;
import jnr.posix.util.Platform;

import org.apache.hadoop.fs.FileStatus;
import ru.serce.jnrfuse.struct.FileStat;

class Attributes {

    private static final Logger LOG = Logger.getLogger(Attributes.class.getName());

    // uid/gid are overwritten by fuse mount options -ouid=...
    private static final int DUMMY_UID = 65534; // usually nobody
    private static final int DUMMY_GID = 65534; // usually nobody

    public static void copy(FileStatus status, FileStat stat) {
        short fallback = 0;
        int filetype = FileStat.S_IFREG;
        if (status.isFile()) {
            fallback = (short) (FileStat.S_IFREG | 0640);
        } else if (status.isDirectory()) {
            fallback = (short) (FileStat.S_IFDIR | 0750);
            filetype = FileStat.S_IFDIR;
        }

        // try using the permissions short from hadoop.
        int mode = (status.getPermission() == null ? fallback : filetype | status.getPermission().toShort());
        LOG.log(Level.INFO, "MODE is {0} hex {1} oct {2} for {3}", new Object[]{mode, Integer.toHexString(mode), Integer.toOctalString(mode), status.getPath().getName()});
        stat.st_mode.set(mode);

        stat.st_uid.set(DUMMY_UID);
        stat.st_gid.set(DUMMY_GID);

        stat.st_size.set(status.getLen());

        Instant modified = Instant.ofEpochMilli(status.getModificationTime());
        stat.st_mtim.tv_sec.set(modified.getEpochSecond());
        stat.st_mtim.tv_nsec.set(modified.getNano());

        if (Platform.IS_MAC || Platform.IS_WINDOWS) {
            assert stat.st_birthtime != null;
            stat.st_birthtime.tv_sec.set(modified.getEpochSecond());
            stat.st_birthtime.tv_nsec.set(modified.getNano());
        }
        stat.st_nlink.set(1);

        // make sure to nil certain fields known to contain garbage from uninitialized memory
        // fixes alleged permission bugs, see https://github.com/cryptomator/fuse-nio-adapter/issues/19
        if (Platform.IS_MAC) {
            stat.st_flags.set(0);
            stat.st_gen.set(0);
        }
    }

    public static Set<OpenFlags> opening(long openflags) {

        EnumSet<OpenFlags> flags = EnumSet.noneOf(OpenFlags.class);

        // these from fedora linux /usr/include/bits/fcntl-linux.h
        if ((openflags & 1) > 0) {
            flags.add(OpenFlags.O_WRONLY);
        } else {
            flags.add(OpenFlags.O_RDONLY);
        }
        if ((openflags & 2) > 0) {
            flags.add(OpenFlags.O_RDWR);
        }

        if ((openflags & 0100) > 0) {
            flags.add(OpenFlags.O_CREAT);
        }
        if ((openflags & 0200) > 0) {
            flags.add(OpenFlags.O_EXCL);
        }
        if ((openflags & 0400) > 0) {
            flags.add(OpenFlags.O_NOCTTY);
        }
        if ((openflags & 01000) > 0) {
            flags.add(OpenFlags.O_TRUNC);
        }
        if ((openflags & 02000) > 0) {
            flags.add(OpenFlags.O_APPEND);
        }
        if ((openflags & 04000) > 0) {
            flags.add(OpenFlags.O_NONBLOCK);
        }

        if ((openflags & 04000000) > 0 && (openflags & 010000) > 0) {
            flags.add(OpenFlags.O_FSYNC);
        }
        if ((openflags & 020000) > 0) {
            flags.add(OpenFlags.O_ASYNC);
        }

        if ((openflags & 0100000) > 0) {
            //flags.add(OpenFlags.O_LARGEFILE);
        }
        if ((openflags & 0200000) > 0) {
            flags.add(OpenFlags.O_DIRECTORY);
        }
        if ((openflags & 0400000) > 0) {
            flags.add(OpenFlags.O_NOFOLLOW);
        }
        if ((openflags & 02000000) > 0) {
            flags.add(OpenFlags.O_CLOEXEC);
        }
        if ((openflags & 040000) > 0) {
            //flags.add(OpenFlags.O_DIRECT);
        }
        if ((openflags & 01000000) > 0) {
            //flags.add(OpenFlags.O_NOATIME);
        }
        if ((openflags & 010000000) > 0) {
            //flags.add(OpenFlags.O_PATH);
        }
        if ((openflags & 010000) > 0) {
            //flags.add(OpenFlags.O_DSYNC);
        }
        if ((openflags & 020000000) > 0 && (openflags & 0200000) > 0) {
            flags.add(OpenFlags.O_TMPFILE);
        }

        // 2 POSIX flags not known to fedora.
        // https://unix.superglobalmegacorp.com/Net2/newsrc/sys/fcntl.h.html
        if ((openflags & 0x10) > 0) {
            flags.add(OpenFlags.O_SHLOCK);
        }
        if ((openflags & 0x20) > 0) {
            flags.add(OpenFlags.O_EXLOCK);
        }

        return flags;
    }
}
