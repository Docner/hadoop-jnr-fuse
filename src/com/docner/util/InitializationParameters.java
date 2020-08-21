package com.docner.util;

import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

@SuppressWarnings("serial")
public class InitializationParameters extends Properties {

    private final static Logger LOG = Logger.getLogger(InitializationParameters.class.getName());

    public InitializationParameters() {
        super();
    }

    public InitializationParameters(Properties properties) {
        super(properties);
    }

    public String get(Package inPackage, String key) {
        return getProperty(inPackage.getName() + "." + key);
    }

    public String get(Object inPackageForClassForObject, String key) {
        return getProperty(inPackageForClassForObject.getClass().getPackage().getName() + "." + key);
    }

    public InitializationParameters set(Package inPackage, String key, String value) {
        setProperty(inPackage.getName() + "." + key, value);
        return this;
    }

    public InitializationParameters set(Object inPackageForClassForObject, String key, String value) {
        setProperty(inPackageForClassForObject.getClass().getPackage().getName() + "." + key, value);
        return this;
    }

    public InitializationParameters fallback(Object inPackageForClassForObject, String key,
            String value) {
        if (get(inPackageForClassForObject, key) == null) {
            set(inPackageForClassForObject, key, value);
        }
        return this;
    }

    public Integer getInt(Object anchor, String propertyName) throws NumberFormatException {
        String value = get(anchor, propertyName);
        if (value == null) {
            return null;
        } else {
            return Integer.parseInt(value);
        }
    }

    public int getInt(Object anchor, String propertyName, int fallback) {
        fallback(anchor, propertyName, Integer.toString(fallback));

        int found;
        try {
            Integer configured = getInt(anchor, propertyName);
            found = configured;
        } catch (NumberFormatException nfe) {
            found = fallback;
            LOG.log(Level.WARNING, "{0} is configured with an invalid value; fallback to {1}", new Object[]{propertyName, found});
        }
        return found;
    }

    public Long getLong(Object inPackageForClassForObject, String key) throws NumberFormatException {
        String value = get(inPackageForClassForObject, key);
        if (value == null) {
            return null;
        } else {
            return Long.parseLong(value);
        }
    }

    public long getLong(Object anchor, String propertyName, long fallback) {
        fallback(anchor, propertyName, Long.toString(fallback));

        long found;
        try {
            Integer configured = getInt(anchor, propertyName);
            found = configured;
        } catch (NumberFormatException nfe) {
            found = fallback;
            LOG.log(Level.WARNING, "{0} is configured with an invalid value; fallback to {1}", new Object[]{propertyName, found});
        }
        return found;
    }

    public String getString(Object anchor, String propertyName, String fallback) {
        fallback(anchor, propertyName, fallback);
        return get(anchor, propertyName);
    }
}
