package com.syncstream.registry.service;

public class ValidationResult {
    private final boolean valid;
    private final String message;

    private ValidationResult(boolean valid, String message) {
        this.valid = valid;
        this.message = message;
    }

    public static ValidationResult ok() {
        return new ValidationResult(true, "");
    }

    public static ValidationResult invalid(String message) {
        return new ValidationResult(false, message);
    }

    public boolean valid() {
        return valid;
    }

    public String message() {
        return message;
    }
}
