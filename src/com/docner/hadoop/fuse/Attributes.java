package com.docner.hadoop.fuse;

import java.time.Instant;
import java.util.logging.Level;
import java.util.logging.Logger;
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
}
