package com.syncstream.redissync.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DebeziumEnvelopeTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void opHelpersShouldWorkForCreateSnapshotUpdateDelete() throws Exception {
        DebeziumEnvelope create = new DebeziumEnvelope();
        create.setOp("c");
        create.setAfter(mapper.readTree("{\"id\":1}"));

        DebeziumEnvelope snapshot = new DebeziumEnvelope();
        snapshot.setOp("r");

        DebeziumEnvelope update = new DebeziumEnvelope();
        update.setOp("u");

        DebeziumEnvelope delete = new DebeziumEnvelope();
        delete.setOp("d");

        assertTrue(create.isCreateOrSnapshot());
        assertTrue(snapshot.isCreateOrSnapshot());
        assertFalse(update.isCreateOrSnapshot());

        assertTrue(update.isUpdate());
        assertFalse(create.isUpdate());

        assertTrue(delete.isDelete());
        assertFalse(snapshot.isDelete());
    }
}
