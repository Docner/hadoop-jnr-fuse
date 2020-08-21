package com.docner.hadoop.fuse.mount;

import com.docner.hadoop.fuse.HDFSFuseAdapter;
import com.docner.hadoop.fuse.HDFSFuseAdapterInitializer;
import static com.docner.hadoop.fuse.mount.SingleMount.parseArguments;
import com.docner.util.InitializationParameters;
import com.docner.util.Initializer;
import static com.docner.util.Lookup.optional;
import static com.docner.util.Lookup.register;
import java.io.IOException;
import static java.util.Arrays.asList;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author wiebe
 */
public class SingleMountService {

    private final static Logger LOG = Logger.getLogger(SingleMountService.class.getName());

    CountDownLatch latch;

    public static void stopService(String[] args) {
        try {
            LOG.info("Stopping server...");
            HDFSFuseAdapter adapter = optional(HDFSFuseAdapter.class);
            if (adapter == null) {
                LOG.warning("Fuse adapter not found, stopping nothing.");
            } else {
                adapter.umount();
                adapter.close();
            }
            SingleMountService service = optional(SingleMountService.class);
            if (service == null) {
                LOG.warning("Service not found");
            } else {
                service.latch.countDown();
            }
        } catch (IOException | RuntimeException ex) {
            LOG.log(Level.SEVERE, "Error stopping service", ex);
        } finally {
            System.exit(0);
        }
    }

    public static void startService(String[] args) throws Initializer.InitializationException {
        SingleMountService service = new SingleMountService();
        register(SingleMountService.class, service);

        HDFSFuseAdapterInitializer init = new HDFSFuseAdapterInitializer();
        InitializationParameters params = new InitializationParameters(System.getProperties());
        register(InitializationParameters.class, params);

        params.fallback(init, "hdfsUrl", "hdfs://192.168.88.226:8020/");
        params.fallback(init, "mountPath", "/mnt/hdfs");
        params.fallback(init, "useStartupLoginUser", "true");
        params.set(init, "fuseBlocking", "true");

        String[] fuseOptions = parseArguments(args, params, init);
        LOG.log(Level.INFO, "Connecting to: {0} with fuse options {1}", new Object[]{init.hdfsUrl(), asList(fuseOptions)});

        service.latch = new CountDownLatch(1);
        params.set(init, "fuseBlocking", "true");
        init.initialize(fuseOptions);
        service.latch.countDown();
        try {
            service.latch.await();
        } catch (InterruptedException ex) {
            SingleMountService.stopService(new String[0]);
        }
    }

    public static void main(String[] args) throws Initializer.InitializationException {
        if (args.length > 0 && "stop".equals(args[0])) {
            stopService(args);
        } else {
            startService(args);
        }
    }
}
