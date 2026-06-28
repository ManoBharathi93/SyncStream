package com.syncstream.registry;

import com.syncstream.registry.api.DisableConsumerRequest;
import com.syncstream.registry.api.RegisterConsumerRequest;
import com.syncstream.registry.api.UpdateConsumerRequest;
import com.syncstream.registry.model.ConsumerRegistration;
import com.syncstream.registry.model.RegistrationStatus;
import com.syncstream.registry.service.RegistryException;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RegistryServiceTest {

    @Test
    void registerShouldBeIdempotentOnConsumerTopicEnvironment() {
        TestBootstrap.Context context = TestBootstrap.context();
        RegisterConsumerRequest request = registerRequest();

        ConsumerRegistration first = context.registryService.register(request);
        ConsumerRegistration second = context.registryService.register(request);

        assertEquals(first.id(), second.id());
        assertTrue(second.version() >= first.version());
    }

    @Test
    void registerShouldRejectPolicyViolatingTopic() {
        TestBootstrap.Context context = TestBootstrap.context();
        RegisterConsumerRequest request = registerRequest();
        request.topic = "orders";

        RegistryException ex = assertThrows(RegistryException.class, () -> context.registryService.register(request));
        assertEquals(400, ex.statusCode());
    }

    @Test
    void updateShouldEnforceOptimisticConcurrency() {
        TestBootstrap.Context context = TestBootstrap.context();
        ConsumerRegistration created = context.registryService.register(registerRequest());

        UpdateConsumerRequest update = new UpdateConsumerRequest();
        update.actor = "team-analytics";
        update.expectedVersion = 999L;
        update.ownerTeam = "team-analytics";
        update.config = Collections.<String, Object>emptyMap();

        RegistryException ex = assertThrows(RegistryException.class, () -> context.registryService.update(created.id(), update));
        assertEquals(409, ex.statusCode());
    }

    @Test
    void disableShouldTransitionStatus() {
        TestBootstrap.Context context = TestBootstrap.context();
        ConsumerRegistration created = context.registryService.register(registerRequest());

        DisableConsumerRequest request = new DisableConsumerRequest();
        request.actor = "team-analytics";
        request.expectedVersion = created.version() + 1; // reconciler makes it ACTIVE with bumped version

        ConsumerRegistration disabled = context.registryService.disable(created.id(), request);
        assertEquals(RegistrationStatus.DISABLED, disabled.status());
    }

    @Test
    void registrationShouldCreateRouteState() throws Exception {
        TestBootstrap.Context context = TestBootstrap.context();
        context.registryService.register(registerRequest());

        List<?> routes = context.routeQueryService.listRoutes();
        assertTrue(routes.size() >= 1);
    }

    @Test
    void shouldExposeAuditHistory() {
        TestBootstrap.Context context = TestBootstrap.context();
        ConsumerRegistration created = context.registryService.register(registerRequest());

        List<?> history = context.registryService.history(created.id());
        assertTrue(history.size() >= 1);
    }

    private RegisterConsumerRequest registerRequest() {
        RegisterConsumerRequest request = new RegisterConsumerRequest();
        request.consumer = "analytics";
        request.topic = "syncstream.orders";
        request.environment = "dev";
        request.ownerTeam = "team-analytics";
        request.actor = "team-analytics";
        Map<String, Object> config = new HashMap<String, Object>();
        config.put("batchSize", 500);
        request.config = config;
        return request;
    }
}
