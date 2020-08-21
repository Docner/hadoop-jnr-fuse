package com.docner.util;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Service lookup that uses a naive lookup by default but can be configured to
 * use context-dependent lookup providers. It is still only meant as a place to
 * register service-type singleton objects, providers may for instance provide a
 * lookup per user-login or per Project configuration.
 *
 * @author wiebe
 */
public class Lookup {

    private static final Logger LOG = Logger.getLogger(Lookup.class.getName());
    private static LookupProvider provider;
    public static final String PROP_PROVIDER = "provider";
    private static PropertyChangeSupport propsup;

    @SuppressWarnings("unchecked")
    public static <T> T register(Class<T> type, T implementation) {
        return provider().register(type, implementation);
    }

    public static <T> List<T> lookupOnClassPath(Class<T> type) {
        return provider().lookupOnClassPath(type);
    }

    public static <T> T simpleChainFromClassPath(Class<T> type) {
        return provider().simpleChainFromClassPath(type);
    }

    /**
     * Finds a service assuming that it will exist. Use this only when you are
     * really sure that the service has been registered before. Like using an
     * assert() it will throw a Runtime exception to indicate failure to do so.
     * If the service may or may not be registered, it is more appropriate to
     * use the 'optional' accessor.
     *
     * @param <T>
     * @param type
     * @return
     * @throws IllegalStateException
     */
    @SuppressWarnings("unchecked")
    public static <T> T require(Class<T> type) throws IllegalStateException {
        return provider().require(type);
    }

    @SuppressWarnings("unchecked")
    public static <T> T optional(Class<T> type) {
        return provider().optional(type);
    }

    @SuppressWarnings("unchecked")
    public static <T> T clear(Class<T> type) {
        return provider().clear(type);
    }

    public static void clear() {
        provider().clear();
    }

    static Map<Class<?>, Object> services() {
        return provider().services();
    }

    static PropertyChangeSupport propertyChangeSupport() {
        if (propsup == null) {
            propsup = new PropertyChangeSupport(Lookup.class);
        }
        return propsup;
    }

    public static void addPropertyChangeListener(PropertyChangeListener l) {
        propertyChangeSupport().addPropertyChangeListener(l);
        provider().addPropertyChangeListener(l);
    }

    public static void removePropertyChangeListener(PropertyChangeListener l) {
        propertyChangeSupport().addPropertyChangeListener(l);
        provider().removePropertyChangeListener(l);
    }

    public static LookupProvider getProvider() {
        return provider;
    }

    protected static LookupProvider provider() {
        if (provider == null) {
            provider = new SingleLookup();
        }
        return provider;
    }

    public static void setProvider(LookupProvider provider) {
        LookupProvider oldProvider = Lookup.provider;
        Lookup.provider = provider;
        propertyChangeSupport().firePropertyChange(PROP_PROVIDER, oldProvider, provider);
    }

    public interface Delegating<T extends Object> {

        public T getDelegate();

        public void setDelegate(T delegate);
    }

    public interface ProviderOrdering {

        int getOrder();

        public class HigherIsBetter implements Comparator<ProviderOrdering> {

            @Override
            public int compare(ProviderOrdering t, ProviderOrdering t1) {
                return t1.getOrder() - t.getOrder();
            }
        }
    }

    public interface LookupProvider {

        <T> T register(Class<T> type, T implementation);

        <T> List<T> lookupOnClassPath(Class<T> type);

        <T> T simpleChainFromClassPath(Class<T> type);

        <T> T require(Class<T> type) throws IllegalStateException;

        <T> T optional(Class<T> type);

        <T> T clear(Class<T> type);

        void clear();

        Map<Class<?>, Object> services();

        void addPropertyChangeListener(PropertyChangeListener l);

        void removePropertyChangeListener(PropertyChangeListener l);
    }
}
