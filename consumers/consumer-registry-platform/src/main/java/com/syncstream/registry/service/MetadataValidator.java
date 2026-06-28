package com.syncstream.registry.service;

import java.util.Map;
import java.util.regex.Pattern;

public class MetadataValidator {
    private static final Pattern NAME_PATTERN = Pattern.compile("^[a-z0-9][a-z0-9-_.]{1,62}$");
    private static final Pattern TOPIC_PATTERN = Pattern.compile("^[a-z0-9][a-z0-9-_.]{1,100}$");

    public ValidationResult validateRegistration(String consumer, String topic, String environment, String ownerTeam, Map<String, Object> config) {
        if (!matches(consumer, NAME_PATTERN)) {
            return ValidationResult.invalid("consumer must match ^[a-z0-9][a-z0-9-_.]{1,62}$");
        }
        if (!matches(topic, TOPIC_PATTERN)) {
            return ValidationResult.invalid("topic must match ^[a-z0-9][a-z0-9-_.]{1,100}$");
        }
        if (!matches(environment, NAME_PATTERN)) {
            return ValidationResult.invalid("environment must match ^[a-z0-9][a-z0-9-_.]{1,62}$");
        }
        if (!matches(ownerTeam, NAME_PATTERN)) {
            return ValidationResult.invalid("ownerTeam must match ^[a-z0-9][a-z0-9-_.]{1,62}$");
        }
        if (config == null) {
            return ValidationResult.invalid("config must not be null");
        }
        return ValidationResult.ok();
    }

    private boolean matches(String value, Pattern pattern) {
        return value != null && pattern.matcher(value).matches();
    }
}
