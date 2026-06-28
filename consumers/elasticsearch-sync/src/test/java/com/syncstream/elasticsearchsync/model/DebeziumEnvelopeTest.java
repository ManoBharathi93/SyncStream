package com.syncstream.elasticsearchsync.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DebeziumEnvelopeTest {

    @Test
    void isDeleteShouldReflectOperationCode() {
        DebeziumEnvelope delete = new DebeziumEnvelope();
        delete.setOp("d");

        DebeziumEnvelope update = new DebeziumEnvelope();
        update.setOp("u");

        assertTrue(delete.isDelete());
        assertFalse(update.isDelete());
    }
}
