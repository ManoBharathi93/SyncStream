package com.syncstream.registry.service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class PlatformMetrics {
    private final AtomicLong registrationCount = new AtomicLong(0);
    private final AtomicLong registrationLatencyMsTotal = new AtomicLong(0);
    private final AtomicLong provisioningSuccessCount = new AtomicLong(0);
    private final AtomicLong provisioningFailureCount = new AtomicLong(0);
    private final AtomicLong reconcileLagMs = new AtomicLong(0);

    public void observeRegistrationLatency(long latencyMs) {
        registrationCount.incrementAndGet();
        registrationLatencyMsTotal.addAndGet(Math.max(0, latencyMs));
    }

    public void markProvisioningSuccess() {
        provisioningSuccessCount.incrementAndGet();
    }

    public void markProvisioningFailure() {
        provisioningFailureCount.incrementAndGet();
    }

    public void setReconcileLagMs(long lagMs) {
        reconcileLagMs.set(Math.max(0, lagMs));
    }

    public Map<String, Object> snapshot() {
        Map<String, Object> metrics = new HashMap<String, Object>();
        long count = registrationCount.get();
        metrics.put("registrationCount", count);
        metrics.put("registrationLatencyMsAvg", count == 0 ? 0 : registrationLatencyMsTotal.get() / count);
        metrics.put("provisioningSuccessCount", provisioningSuccessCount.get());
        metrics.put("provisioningFailureCount", provisioningFailureCount.get());
        metrics.put("reconcileLagMs", reconcileLagMs.get());
        return metrics;
    }
}
