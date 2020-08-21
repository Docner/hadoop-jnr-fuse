package com.docner.hadoop.fuse;

import com.docner.util.FlexibleURLStreamHandlerFactory;
import com.docner.util.InitializationParameters;
import com.docner.util.Initializer;
import com.docner.util.Initializer.InitializationException;
import static com.docner.util.Lookup.register;
import static com.docner.util.Lookup.require;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLStreamHandler;
import java.nio.file.Paths;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumFileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;

/**
 *
 * @author wiebe
 */
public class HDFSFuseAdapterInitializer implements Initializer {

    private final static Logger LOG = Logger.getLogger(HDFSFuseAdapterInitializer.class.getName());

    @Override
    public void initialize(Object o) throws InitializationException {
        UserGroupInformation login = login();
        String hdfsSpec = hdfsUrl();
        boolean fuseDebug = fuseDebug();
        boolean fuseBlocking = fuseBlocking();
        String[] fuseOptions = new String[0];
        if (o != null && o instanceof String[]) {
            fuseOptions = (String[]) o;
        }

        String username = login.getUserName();
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsSpec);
        conf.set("hadoop.job.ugi", username);

        if (FlexibleURLStreamHandlerFactory.isInstance() || FlexibleURLStreamHandlerFactory.install()) {
            URLStreamHandler handler = new FsUrlStreamHandlerFactory(conf).createURLStreamHandler("hdfs");
            FlexibleURLStreamHandlerFactory.getInstance().put("hdfs", handler);
            LOG.info("HDFS handler installed in flexible factory.");
        } else {
            try {
                URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory(conf));
            } catch (Error alreadySet) {
                if (!"factory already defined".equals(alreadySet.getMessage())) {
                    throw new InitializationException(alreadySet);
                }
            }
        }
        Path home;
        try {
            home = new Path(new URI(hdfsSpec));
            if (home.depth() == 0) {
                home = new Path("/home/" + username);
            }
        } catch (URISyntaxException uex) {
            throw new InitializationException(uex);
        }

        ensureDirectory(conf, home, username);
        HDFSFuseAdapter adapter = makeAdapterWithDirectory(login, conf, username, home);

        adapter.mount(Paths.get(mountPath()), fuseBlocking, fuseDebug, fuseOptions);
    }

    protected HDFSFuseAdapter makeAdapterWithDirectory(UserGroupInformation login, Configuration conf, String username, Path home) throws InitializationException {
        try {
            return login.doAs((PrivilegedExceptionAction<HDFSFuseAdapter>) () -> {
                FileSystem fs = FileSystem.get(conf);
                if (fs instanceof DistributedFileSystem) {
                    ((DistributedFileSystem) fs).setWriteChecksum(true);
                } else if (fs instanceof ChecksumFileSystem) {
                    ((ChecksumFileSystem) fs).setWriteChecksum(true);
                }

                FileStatus[] status = fs.listStatus(home);
                LOG.log(Level.INFO, "Home directory for user {0} is {1}", new Object[]{username, home});
                LOG.log(Level.INFO, "Files in home directory: {0}", Arrays.asList(status).toString());

                HDFSFuseAdapter adapter = new HDFSFuseAdapter(home, 255, fs, conf, login);
                register(HDFSFuseAdapter.class, adapter);

                return adapter;
            });
        } catch (IOException | InterruptedException ex) {
            throw new InitializationException(ex);
        }
    }

    protected void ensureDirectory(Configuration conf, final Path homeDir, String username) throws InitializationException {
        try {
            hadoop().doAs((PrivilegedExceptionAction<Void>) () -> {
                Configuration forHadoop = new Configuration(conf);
                conf.set("hadoop.job.ugi", "hadoop");

                FileSystem fs = FileSystem.get(conf);
                try {
                    fs.getFileStatus(homeDir);
                } catch (FileNotFoundException fnf) {
                    boolean created = fs.mkdirs(homeDir);
                    if (!created) {
                        throw new IOException("Cannot create " + homeDir.toString());
                    }
                }
                try {
                    fs.setOwner(homeDir, username, username);
                } catch (org.apache.hadoop.security.AccessControlException nope) {
                    LOG.log(Level.WARNING, "Cannot set ownership of {0} to {1}. Ignoring for now, but these permissions are important for the application to work.", new Object[]{homeDir, username});
                }

                return null;
            });
        } catch (IOException | InterruptedException ioe) {
            throw new InitializationException(ioe);
        }
    }

    UserGroupInformation hadoop() {
        return UserGroupInformation.createRemoteUser("hadoop");
    }

    UserGroupInformation login() {
        UserGroupInformation login = null;
        InitializationParameters params = require(InitializationParameters.class);
        String useUser = params.get(this, "useStartupLoginUser");
        if (useUser != null && "true".equalsIgnoreCase(useUser)) {
            try {
                login = UserGroupInformation.getLoginUser();
            } catch (IOException ex) {
                LOG.log(Level.WARNING, "Cannot read login user while configured to do so. Skipping now but this may cause trouble later.", ex);
            }
        } else {
            params.fallback(this, "hdfsUser", "someone");
            useUser = params.get(this, "hdfsUser");
            login = UserGroupInformation.createRemoteUser(useUser);
        }
        register(UserGroupInformation.class, login);
        return login;
    }

    public String hdfsUrl() {
        String spec = "hdfs://localhost:8020/";
        InitializationParameters params = require(InitializationParameters.class);
        params.fallback(this, "hdfsUrl", spec);
        spec = params.get(this, "hdfsUrl");
        return spec;
    }

    public String mountPath() {
        String spec = "/mnt/hdfs";
        InitializationParameters params = require(InitializationParameters.class);
        params.fallback(this, "mountPath", spec);
        spec = params.get(this, "mountPath");
        LOG.log(Level.INFO, "Mounting on {0}", spec);
        return spec;
    }

    public boolean fuseDebug() {
        return readBoolean("fuseDebug", false);
    }

    public boolean fuseBlocking() {
        return readBoolean("fuseBlocking", false);
    }

    public boolean readBoolean(String name, boolean defaultValue) {
        String spec = Boolean.toString(defaultValue);
        InitializationParameters params = require(InitializationParameters.class);
        params.fallback(this, name, spec);
        spec = params.get(this, name);
        LOG.log(Level.INFO, "With FUSE {1} {0}", new Object[]{spec, name});
        return "true".equalsIgnoreCase(spec);
    }
}
