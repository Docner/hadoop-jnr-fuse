/*
 * FlexibleURLStreamHandlerFactory.java
 *
 * Created on July 22, 2006, 10:38 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */
package com.docner.util;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Maintains alternative URL handlers. Logs to a logger named
 * 'com.docner.util.FlexibleURLStreamHandlerFactory' on FINE level.
 *
 * @author wiebe
 */
public class FlexibleURLStreamHandlerFactory implements URLStreamHandlerFactory {

    private static Logger log = Logger.getLogger(FlexibleURLStreamHandlerFactory.class.getName());
    protected Map<String, URLStreamHandler> handlers;
    protected static FlexibleURLStreamHandlerFactory theOne;

    /**
     * Creates a new instance of FlexibleURLStreamHandlerFactory
     */
    public FlexibleURLStreamHandlerFactory() {
        initMap();
    }

    public FlexibleURLStreamHandlerFactory(String packages) {
        setPackages(packages);
        initMap();
    }

    protected void initMap() {
        if (handlers == null) {
            handlers = Collections.synchronizedMap(new HashMap<String, URLStreamHandler>());
        }

        handlers.clear();
        handlers.put("check", new FlexibleURLStreamHandlerCheckHandler(this));
    }

    @Override
    public URLStreamHandler createURLStreamHandler(String protocol) {

        if (protocol == null) {
            return null;
        }

        URLStreamHandler result = handlers.get(protocol);
        if (result != null) {
            log.log(Level.FINE, "createURLStreamHandler found cached handler for {0}", protocol);
            return result;
        }
        log.log(Level.FINE, "createURLStreamHandler delves deeper for {0}", protocol);
        ClassLoader contextcl = Thread.currentThread().getContextClassLoader();
        StringTokenizer packs = new StringTokenizer(getPackages(), "|");
        while (packs.hasMoreTokens()) {
            String packge = packs.nextToken();
            String classname = packge + "." + protocol + ".Handler";
            Class found = null;

            try {
                found = contextcl.loadClass(classname);
            } catch (ClassNotFoundException e) {
                try {
                    found = Class.forName(classname);
                } catch (ClassNotFoundException cnf) {
                    found = null;
                }
            }

            if (found != null) {
                log.log(Level.FINE, "createURLStreamHandler found {0} for {1}", new Object[]{found.getName(), protocol});
                try {
                    URLStreamHandler handler = (URLStreamHandler) found.newInstance();
                    handlers.put(protocol, handler);
                    return handler;
                } catch (InstantiationException iae) {
                    iae.printStackTrace();
                } catch (IllegalAccessException iae) {
                    iae.printStackTrace();
                }
            }
        }

        return null;
    }

    public void put(String protocol, URLStreamHandler handler) {
        handlers.put(protocol, handler);
    }

    public static boolean install() {
        FlexibleURLStreamHandlerFactory check = theOne;
        if (check == null) {
            check = new FlexibleURLStreamHandlerFactory();
            theOne = check;
        }
        URL.setURLStreamHandlerFactory(theOne);
        if (isInstalled()) {
            Lookup.register(FlexibleURLStreamHandlerFactory.class, theOne);
            return true;
        }
        return false;
    }

    public static boolean isInstalled() {
        try {
            URL checkit = new URL("check", "", -1, "");
            URLConnection checkconn = checkit.openConnection();
            if (checkconn instanceof FlexibleURLStreamHandlerCheckConnection) {
                FlexibleURLStreamHandlerCheckConnection conn = (FlexibleURLStreamHandlerCheckConnection) checkconn;
                FlexibleURLStreamHandlerFactory check = conn.getFactory();
                return (check == theOne);
            }
        } catch (IOException ioe) {
            log.log(Level.WARNING, "Cannot create a 'check' protocol URL, so the flexible url stream handler factory is not installed.");
        }
        return false;
    }

    public static boolean isInstance() {
        try {
            URL checkit = new URL("check", "", -1, "");
            URLConnection checkconn = checkit.openConnection();
            if (checkconn instanceof FlexibleURLStreamHandlerCheckConnection) {
                FlexibleURLStreamHandlerCheckConnection conn = (FlexibleURLStreamHandlerCheckConnection) checkconn;
                FlexibleURLStreamHandlerFactory check = conn.getFactory();
                return check != null;
            }
        } catch (IOException ioe) {
            log.log(Level.WARNING, "Cannot create a 'check' protocol URL, so the flexible url stream handler factory is not installed.");
        }
        return false;
    }

    public static FlexibleURLStreamHandlerFactory getInstance() {
        FlexibleURLStreamHandlerFactory result = theOne;
        if (theOne == null) {
            install();
        }
        return theOne;
    }

    public static class FlexibleURLStreamHandlerCheckHandler extends URLStreamHandler {

        private FlexibleURLStreamHandlerFactory fact;

        public FlexibleURLStreamHandlerCheckHandler(FlexibleURLStreamHandlerFactory factory) {
            this.fact = factory;
        }

        protected URLConnection openConnection(URL u) throws IOException {
            return new FlexibleURLStreamHandlerCheckConnection(u, fact);
        }

        public FlexibleURLStreamHandlerFactory getFactory() {
            return fact;
        }
    }

    public static class FlexibleURLStreamHandlerCheckConnection extends URLConnection {

        private FlexibleURLStreamHandlerFactory fact;

        public FlexibleURLStreamHandlerCheckConnection(URL url, FlexibleURLStreamHandlerFactory factory) {
            super(url);
            this.fact = factory;
        }

        public FlexibleURLStreamHandlerFactory getFactory() {
            return fact;
        }

        public void connect() throws IOException {
        }
    }
    /**
     * Holds value of property packages.
     */
    private String packages;

    /**
     * Getter for property packages.
     *
     * @return Value of property packages.
     */
    public String getPackages() {
        if (this.packages == null) {
            this.packages = "";
        }
        return this.packages;
    }

    /**
     * Setter for property packages. This will reload the internal protocol map,
     * reversing any calls to put(...).
     *
     * @param packages New value of property packages.
     */
    public void setPackages(String packages) {
        this.packages = packages;
        initMap();
    }
}
