package com.docner.util;

/**
 *
 * @author wiebe
 */
public interface Initializer {

    void initialize(Object config) throws InitializationException;

    public static class InitializationException extends Exception {

        public InitializationException() {
        }

        public InitializationException(String message) {
            super(message);
        }

        public InitializationException(Throwable cause) {
            super(cause);
        }

        public InitializationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
