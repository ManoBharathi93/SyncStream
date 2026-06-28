package com.syncstream.registry.service;

import com.syncstream.registry.model.RegistryEvent;

import java.util.ArrayList;
import java.util.List;

public class RegistryEventBus {
    public interface Listener {
        void onEvent(RegistryEvent event);
    }

    private final List<Listener> listeners = new ArrayList<Listener>();

    public synchronized void register(Listener listener) {
        listeners.add(listener);
    }

    public synchronized void publish(RegistryEvent event) {
        for (Listener listener : listeners) {
            listener.onEvent(event);
        }
    }
}
