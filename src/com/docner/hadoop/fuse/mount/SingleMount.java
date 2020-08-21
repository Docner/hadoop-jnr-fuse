package com.docner.hadoop.fuse.mount;

import com.docner.hadoop.fuse.HDFSFuseAdapterInitializer;
import com.docner.util.InitializationParameters;
import com.docner.util.Initializer;
import static com.docner.util.Lookup.register;
import java.util.ArrayList;
import static java.util.Arrays.asList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Mount entry point to be used from the linux mount command.
 * Only works when giving 'blocking' because the daemon thread
 * exits immediately - can only work when something else keeps 
 * the JVM alive but then the mount command cannot exit..
 * 
 * Better see this as a preparation to create a solution that
 * starts a service when necessary, which service exposes a
 * socket or file or other means of IPC, then signals the service
 * to mount with the options parsed her more or less correctly.
 *
 * @author wiebe
 */
public class SingleMount {

    private final static Logger LOG = Logger.getLogger(SingleMount.class.getName());
    private final static List<String> ALLOWEDOPTIONS = asList(new String[]{
        "fsname", "subtype", "nonempty", "umask", "modules"
    });

    public static void main(String[] args) throws Initializer.InitializationException {
        for (String arg : args) {
            System.out.println("> " + arg);
        }
        HDFSFuseAdapterInitializer init = new HDFSFuseAdapterInitializer();
        InitializationParameters params = new InitializationParameters();
        register(InitializationParameters.class, params);

        params.set(init, "hdfsUrl", "hdfs://localhost:8020/");
        params.set(init, "mountPath", "/mnt/hdfs");
        params.set(init, "hdfsUser", "someone");

        String[] fuseOptions = parseArguments(args, params, init);
        LOG.log(Level.INFO, "Connecting to: {0} with fuse options {1}", new Object[]{init.hdfsUrl(), asList(fuseOptions)});

        init.initialize(fuseOptions);
    }

    public static String[] parseArguments(String[] args, InitializationParameters params, HDFSFuseAdapterInitializer init) throws Initializer.InitializationException {
        String[] fuseOptions = new String[0];
        int pos = 0;
        while (pos < args.length) {
            String arg = args[pos];
            switch (pos) {
                case 0:
                    LOG.log(Level.INFO, "Setting hdfs url to {0}", arg);
                    params.set(init, "hdfsUrl", arg);
                    break;
                case 1:
                    LOG.log(Level.INFO, "Setting mount point to {0}", arg);
                    params.set(init, "mountPath", arg);
                    break;
                default:
                    switch (arg) {
                        case "-o":
                            if (args.length - pos > 1) {
                                String options = args[pos + 1];
                                fuseOptions = parseMountOptions(options, params, init);
                            } else {
                                LOG.info("ignoring the option switch.");
                            }
                            break;
                    }
                    break;
            }
            pos++;
        }
        return fuseOptions;
    }

    protected static String[] parseMountOptions(String options, InitializationParameters params, HDFSFuseAdapterInitializer init) throws Initializer.InitializationException {

        boolean quoted = false;
        List<String> parts = new ArrayList<>();
        for (String q : options.split("\"")) {
            if (quoted) {
                parts.add(q.trim());
            } else {
                for (String s : q.split(",")) {
                    if (!s.trim().isEmpty()) {
                        parts.add(s.trim());
                    }
                }
            }
            quoted = !quoted;
        }
        List<String> fuseOptions = new ArrayList<>();
        int pos = 0;
        while (pos < parts.size()) {
            String part = parts.get(pos);
            if (part.endsWith("=") && parts.size() - pos > 1) {
                part = part + parts.get(pos + 1);
                parts.set(pos + 1, "");
            }
            pos++;
            if (part.startsWith("user=")) {
                String user = part.substring(5);
                LOG.log(Level.INFO, "Setting hadoop user name to {0}", user);
                params.set(init, "hdfsUser", user);
                continue;
            }
            switch (part) {
                case "owner":
                case "login":
                    LOG.info("Using login user and disregard any 'user=' directive even if given.");
                    params.set(init, "useStartupLoginUser", "true");
                    break;
                case "debug":
                    LOG.info("Fuse debug is ON.");
                    params.set(init, "fuseDebug", "true");
                    break;
                case "blocking":
                    LOG.info("Fuse is BLOCKING.");
                    params.set(init, "fuseBlocking", "true");
                    break;
                case "suid":
                    LOG.info("Ignoring 'suid', configuring 'nosuid");
                    fuseOptions.add("nosuid");
                    break;
                case "dev":
                    LOG.info("Ignoring 'dev', configuring 'nodev");
                    fuseOptions.add("nodev");
                    break;
                default:
                    break;
            }
            if (ALLOWEDOPTIONS.contains(part)) {
                fuseOptions.add(part);
            }
            if (part.indexOf('=') > 0) {
                String prefix = part.substring(0, pos);
                if (ALLOWEDOPTIONS.contains(prefix)) {
                    fuseOptions.add(part);
                }
            }
        }
        return fuseOptions.toArray(new String[fuseOptions.size()]);
    }
}
