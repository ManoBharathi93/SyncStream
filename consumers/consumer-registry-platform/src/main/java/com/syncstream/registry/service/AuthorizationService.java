package com.syncstream.registry.service;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class AuthorizationService {
    private final Set<String> viewerRoles = new HashSet<String>(Arrays.asList("viewer", "admin", "platform-admin", "ops"));
    private final Set<String> adminRoles = new HashSet<String>(Arrays.asList("admin", "platform-admin", "ops"));

    public void requireAdmin(String role) {
        if (!adminRoles.contains(normalizeRole(role))) {
            throw new RegistryException(403, "forbidden: admin role required");
        }
    }

    public void requireViewer(String role) {
        if (!viewerRoles.contains(normalizeRole(role))) {
            throw new RegistryException(403, "forbidden: viewer role required");
        }
    }

    private String normalizeRole(String role) {
        return role == null ? "" : role.trim().toLowerCase();
    }
}
