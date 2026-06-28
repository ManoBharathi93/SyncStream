package com.syncstream.registry.service;

import com.syncstream.registry.api.DisableConsumerRequest;
import com.syncstream.registry.api.RegisterConsumerRequest;
import com.syncstream.registry.api.UpdateConsumerRequest;
import com.syncstream.registry.db.AuditRepository;
import com.syncstream.registry.db.DatabaseManager;
import com.syncstream.registry.db.EventRepository;
import com.syncstream.registry.db.RegistryRepository;
import com.syncstream.registry.model.ConsumerRegistration;
import com.syncstream.registry.model.RegistrationAuditRecord;
import com.syncstream.registry.model.RegistrationStatus;
import com.syncstream.registry.model.RegistryEvent;
import com.syncstream.registry.util.Jsons;
import com.syncstream.registry.util.Times;

import java.sql.Connection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class RegistryService {
    private final DatabaseManager databaseManager;
    private final RegistryRepository registryRepository;
    private final AuditRepository auditRepository;
    private final EventRepository eventRepository;
    private final MetadataValidator metadataValidator;
    private final RegistryEventBus eventBus;
    private final PlatformMetrics metrics;

    public RegistryService(
        DatabaseManager databaseManager,
        RegistryRepository registryRepository,
        AuditRepository auditRepository,
        EventRepository eventRepository,
        MetadataValidator metadataValidator,
        RegistryEventBus eventBus,
        PlatformMetrics metrics
    ) {
        this.databaseManager = databaseManager;
        this.registryRepository = registryRepository;
        this.auditRepository = auditRepository;
        this.eventRepository = eventRepository;
        this.metadataValidator = metadataValidator;
        this.eventBus = eventBus;
        this.metrics = metrics;
    }

    public ConsumerRegistration register(RegisterConsumerRequest request) {
        long startMs = System.currentTimeMillis();
        String actor = normalizeActor(request.actor);
        ValidationResult validationResult = metadataValidator.validateRegistration(
            request.consumer,
            request.topic,
            request.environment,
            request.ownerTeam,
            request.config == null ? Collections.<String, Object>emptyMap() : request.config
        );
        if (!validationResult.valid()) {
            throw new RegistryException(400, validationResult.message());
        }

        policyCheckTopic(request.topic);

        try (Connection connection = databaseManager.connection()) {
            connection.setAutoCommit(false);
            try {
                ConsumerRegistration existing = registryRepository.findByUnique(connection, request.consumer, request.topic, request.environment);
                if (existing != null) {
                    connection.commit();
                    metrics.observeRegistrationLatency(System.currentTimeMillis() - startMs);
                    return existing;
                }

                String now = Times.nowIsoUtc();
                ConsumerRegistration created = new ConsumerRegistration(
                    UUID.randomUUID().toString(),
                    request.consumer,
                    request.topic,
                    request.environment,
                    request.ownerTeam,
                    RegistrationStatus.PENDING_PROVISIONING,
                    request.config == null ? Collections.<String, Object>emptyMap() : request.config,
                    1L,
                    now,
                    now,
                    actor,
                    actor
                );
                registryRepository.insert(connection, created);
                addAudit(connection, created.id(), "REGISTER", null, Jsons.toJson(created), actor);
                RegistryEvent event = createEvent(created.id(), "REGISTER", created);
                eventRepository.insert(connection, event);
                connection.commit();
                eventBus.publish(event);
                metrics.observeRegistrationLatency(System.currentTimeMillis() - startMs);
                return created;
            } catch (RegistryException ex) {
                connection.rollback();
                throw ex;
            } catch (Exception ex) {
                connection.rollback();
                throw new IllegalStateException("Failed to register consumer", ex);
            }
        } catch (RegistryException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to register consumer", ex);
        }
    }

    public List<ConsumerRegistration> list(Map<String, String> filters) {
        try (Connection connection = databaseManager.connection()) {
            return registryRepository.list(connection, filters);
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to list registrations", ex);
        }
    }

    public ConsumerRegistration update(String id, UpdateConsumerRequest request) {
        if (request.expectedVersion == null || request.expectedVersion < 1) {
            throw new RegistryException(400, "expectedVersion must be provided and >= 1");
        }

        String actor = normalizeActor(request.actor);

        try (Connection connection = databaseManager.connection()) {
            connection.setAutoCommit(false);
            try {
                ConsumerRegistration existing = ensureFound(connection, id);
                RegistrationStatus status = request.status == null ? existing.status() : parseStatus(request.status);
                Map<String, Object> config = request.config == null || request.config.isEmpty() ? existing.config() : request.config;
                String ownerTeam = request.ownerTeam == null || request.ownerTeam.trim().isEmpty() ? existing.ownerTeam() : request.ownerTeam.trim();

                ValidationResult validationResult = metadataValidator.validateRegistration(
                    existing.consumer(),
                    existing.topic(),
                    existing.environment(),
                    ownerTeam,
                    config
                );
                if (!validationResult.valid()) {
                    throw new RegistryException(400, validationResult.message());
                }

                ConsumerRegistration updated = new ConsumerRegistration(
                    existing.id(),
                    existing.consumer(),
                    existing.topic(),
                    existing.environment(),
                    ownerTeam,
                    status,
                    config,
                    existing.version() + 1,
                    existing.createdAt(),
                    Times.nowIsoUtc(),
                    existing.createdBy(),
                    actor
                );

                boolean updatedOk = registryRepository.updateWithVersion(connection, updated, request.expectedVersion);
                if (!updatedOk) {
                    throw new RegistryException(409, "version conflict");
                }

                addAudit(connection, id, "UPDATE", Jsons.toJson(existing), Jsons.toJson(updated), actor);
                RegistryEvent event = createEvent(id, "UPDATE", updated);
                eventRepository.insert(connection, event);
                connection.commit();
                eventBus.publish(event);
                return updated;
            } catch (RegistryException ex) {
                connection.rollback();
                throw ex;
            } catch (Exception ex) {
                connection.rollback();
                throw new IllegalStateException("Failed to update registration", ex);
            }
        } catch (RegistryException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to update registration", ex);
        }
    }

    public ConsumerRegistration disable(String id, DisableConsumerRequest request) {
        if (request.expectedVersion == null || request.expectedVersion < 1) {
            throw new RegistryException(400, "expectedVersion must be provided and >= 1");
        }

        String actor = normalizeActor(request.actor);

        try (Connection connection = databaseManager.connection()) {
            connection.setAutoCommit(false);
            try {
                ConsumerRegistration existing = ensureFound(connection, id);
                ConsumerRegistration disabled = new ConsumerRegistration(
                    existing.id(),
                    existing.consumer(),
                    existing.topic(),
                    existing.environment(),
                    existing.ownerTeam(),
                    RegistrationStatus.DISABLED,
                    existing.config(),
                    existing.version() + 1,
                    existing.createdAt(),
                    Times.nowIsoUtc(),
                    existing.createdBy(),
                    actor
                );

                boolean updatedOk = registryRepository.updateWithVersion(connection, disabled, request.expectedVersion);
                if (!updatedOk) {
                    throw new RegistryException(409, "version conflict");
                }

                addAudit(connection, id, "DISABLE", Jsons.toJson(existing), Jsons.toJson(disabled), actor);
                RegistryEvent event = createEvent(id, "DISABLE", disabled);
                eventRepository.insert(connection, event);
                connection.commit();
                eventBus.publish(event);
                return disabled;
            } catch (RegistryException ex) {
                connection.rollback();
                throw ex;
            } catch (Exception ex) {
                connection.rollback();
                throw new IllegalStateException("Failed to disable registration", ex);
            }
        } catch (RegistryException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to disable registration", ex);
        }
    }

    public List<RegistrationAuditRecord> history(String registrationId) {
        try (Connection connection = databaseManager.connection()) {
            ensureFound(connection, registrationId);
            return auditRepository.listByRegistrationId(connection, registrationId);
        } catch (RegistryException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to list registration history", ex);
        }
    }

    public Map<String, Object> metrics() {
        return metrics.snapshot();
    }

    private ConsumerRegistration ensureFound(Connection connection, String id) {
        ConsumerRegistration registration = registryRepository.findById(connection, id);
        if (registration == null) {
            throw new RegistryException(404, "registration not found");
        }
        return registration;
    }

    private void addAudit(Connection connection, String registrationId, String action, String beforeJson, String afterJson, String actor) {
        auditRepository.insert(connection, new RegistrationAuditRecord(
            UUID.randomUUID().toString(),
            registrationId,
            action,
            beforeJson,
            afterJson,
            actor,
            Times.nowIsoUtc()
        ));
    }

    private RegistryEvent createEvent(String registrationId, String eventType, ConsumerRegistration registration) {
        return new RegistryEvent(
            UUID.randomUUID().toString(),
            registrationId,
            eventType,
            Jsons.toJson(registration),
            Times.nowIsoUtc()
        );
    }

    private RegistrationStatus parseStatus(String rawStatus) {
        try {
            return RegistrationStatus.valueOf(rawStatus.trim().toUpperCase());
        } catch (Exception ex) {
            throw new RegistryException(400, "status must be one of PENDING_PROVISIONING, ACTIVE, DISABLED, FAILED, DEGRADED");
        }
    }

    private void policyCheckTopic(String topic) {
        if (!topic.startsWith("syncstream.")) {
            throw new RegistryException(400, "policy violation: topic must start with syncstream.");
        }
    }

    private String normalizeActor(String actor) {
        if (actor == null || actor.trim().isEmpty()) {
            return "system";
        }
        return actor.trim();
    }
}
