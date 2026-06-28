package com.syncstream.registry.util;

import java.time.Instant;

public final class Times {
    private Times() {
    }

    public static String nowIsoUtc() {
        return Instant.now().toString();
    }
}
