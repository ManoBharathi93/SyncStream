package com.syncstream.registry.service;

import com.syncstream.registry.model.RouteState;

public interface RoutingProvisioner {
    void apply(RouteState routeState);
}
