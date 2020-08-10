package com.docner.util;

import com.docner.util.Lookup.LookupProvider;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Extremely naive service lookup. One static registry of classes vs.
 * implementations.
 *
 * @author wiebe
 */
public class SingleLookup implements LookupProvider {

    private static final Logger LOG = Logger.getLogger(SingleLookup.class.getName());
    private Map<Class<?>, Object> services;
    private PropertyChangeSupport propsup;

    @SuppressWarnings("unchecked")
    @Override
    public <T> T register(Class<T> type, T implementation) {
        Logger.getLogger(SingleLookup.class.getName()).log(Level.INFO, "Registering {0} {1}", new Object[]{type, implementation});
        T registered = (T) services().put(type, implementation);
        propertyChangeSupport().firePropertyChange("registered", null, implementation);
        return registered;
    }

    @Override
    public <T> List<T> lookupOnClassPath(Class<T> type) {
        LOG.log(Level.INFO, "Looking for {0} on the class path", type);
        ServiceLoader<T> loader = ServiceLoader.load(type);
        List<T> result = new ArrayList<>();
        boolean firstDone = false;
        for (T implementation : loader) {
            if (!firstDone) {
                register(type, implementation);
                firstDone = true;
            }
            @SuppressWarnings("unchecked")
            Class<T> implementationClass = (Class<T>) implementation.getClass();
            register(implementationClass, implementation);
            result.add(implementation);
        }
        return result;
    }

    @Override
    public <T> T simpleChainFromClassPath(Class<T> type) {
        List<T> found = lookupOnClassPath(type);

        if (found.isEmpty()) {
            return null;
        }

        List<T> delegating = new ArrayList<>();
        List<T> delegates = new ArrayList<>();
        for (T candidate : found) {
            if (candidate instanceof Lookup.Delegating) {
                delegating.add(candidate);
            } else {
                delegates.add(candidate);
            }
        }

        T endOfChain = delegates.isEmpty() ? delegating.get(delegating.size() - 1) : delegates.get(0);
        LOG.log(Level.INFO, "End of chain is {0}", endOfChain.getClass().getName());

        T top = endOfChain;
        for (int i = delegating.size() - 1; i >= 0; i--) {
            T nextInLine = top;
            top = delegating.get(i);
            if (top == endOfChain) {
                continue;
            }
            @SuppressWarnings("unchecked")
            Lookup.Delegating<T> element = (Lookup.Delegating<T>) top;
            element.setDelegate(nextInLine);
            LOG.log(Level.INFO, "{0} delegates to {1}", new Object[]{top.getClass().getName(), element.getDelegate().getClass().getName()});
        }
        LOG.log(Level.INFO, "Top of chain is {0}", top.getClass().getName());
        register(type, top);
        return top;
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
    @Override
    public <T> T require(Class<T> type) throws IllegalStateException {
        T result = optional(type);
        if (result == null) {
            throw new IllegalStateException("Service " + type + " not registered.");
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T optional(Class<T> type) {
        return (T) services().get(type);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T clear(Class<T> type) {
        return (T) services().remove(type);
    }

    @Override
    public void clear() {
        if (services != null) {
            services.clear();
        }
    }

    @Override
    public Map<Class<?>, Object> services() {
        if (services == null) {
            services = new IdentityHashMap<>();
        }
        return services;
    }

    PropertyChangeSupport propertyChangeSupport() {
        if (propsup == null) {
            propsup = new PropertyChangeSupport(SingleLookup.class);
        }
        return propsup;
    }

    @Override
    public void addPropertyChangeListener(PropertyChangeListener l) {
        propertyChangeSupport().addPropertyChangeListener("registered", l);
    }

    @Override
    public void removePropertyChangeListener(PropertyChangeListener l) {
        propertyChangeSupport().addPropertyChangeListener("registered", l);
    }
}
