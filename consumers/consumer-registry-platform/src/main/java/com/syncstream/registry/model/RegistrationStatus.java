package com.syncstream.registry.model;

public enum RegistrationStatus {
    PENDING_PROVISIONING,
    ACTIVE,
    DISABLED,
    FAILED,
    DEGRADED;

    public static RegistrationStatus fromDbValue(String value) {
        return RegistrationStatus.valueOf(value);
    }

    public String toDbValue() {
        return name();
    }
}
