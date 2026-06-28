package com.syncstream.registry.service;

public class RegistryException extends RuntimeException {
    private final int statusCode;

    public RegistryException(int statusCode, String message) {
        super(message);
        this.statusCode = statusCode;
    }

    public int statusCode() {
        return statusCode;
    }
}
