package com.syncstream.elasticsearchsync.projection;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.syncstream.elasticsearchsync.model.DebeziumEnvelope;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.message.BasicStatusLine;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ProductIndexProjectorTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private RestClient client;
    private ProductIndexProjector projector;

    @BeforeEach
    void setUp() {
        client = mock(RestClient.class);
        projector = new ProductIndexProjector(client, mapper, "products_v1");
    }

    @Test
    void shouldIgnoreNonProductTopics() throws Exception {
        DebeziumEnvelope event = new DebeziumEnvelope();
        event.setOp("u");
        event.setAfter(mapper.readTree("{\"id\":1}"));

        projector.apply("syncstream.public.orders", "{\"id\":1}", event);

        verify(client, never()).performRequest(any(Request.class));
    }

    @Test
    void shouldSendDeleteRequestOnDeleteEvent() throws Exception {
        Response response = mock(Response.class);
        when(response.getStatusLine()).thenReturn(statusLine(200));
        when(client.performRequest(any(Request.class))).thenReturn(response);

        DebeziumEnvelope event = new DebeziumEnvelope();
        event.setOp("d");

        projector.apply("syncstream.public.products", "{\"payload\":{\"id\":55}}", event);

        ArgumentCaptor<Request> captor = ArgumentCaptor.forClass(Request.class);
        verify(client).performRequest(captor.capture());
        Request request = captor.getValue();
        assertEquals("DELETE", request.getMethod());
        assertEquals("/products_v1/_doc/55", request.getEndpoint());
    }

    @Test
    void shouldThrowForUnexpectedDeleteStatus() throws Exception {
        Response response = mock(Response.class);
        when(response.getStatusLine()).thenReturn(statusLine(500));
        when(client.performRequest(any(Request.class))).thenReturn(response);

        DebeziumEnvelope event = new DebeziumEnvelope();
        event.setOp("d");

        assertThrows(IllegalStateException.class,
            () -> projector.apply("syncstream.public.products", "{\"id\":5}", event));
    }

    @Test
    void shouldSendUpsertRequestForCreateUpdateSnapshot() throws Exception {
        Response response = mock(Response.class);
        when(response.getStatusLine()).thenReturn(statusLine(201));
        when(client.performRequest(any(Request.class))).thenReturn(response);

        DebeziumEnvelope event = new DebeziumEnvelope();
        event.setOp("u");
        event.setAfter(mapper.readTree("{\"id\":66,\"name\":\"Desk\"}"));

        projector.apply("syncstream.public.products", "66", event);

        ArgumentCaptor<Request> captor = ArgumentCaptor.forClass(Request.class);
        verify(client).performRequest(captor.capture());
        Request request = captor.getValue();
        assertEquals("PUT", request.getMethod());
        assertEquals("/products_v1/_doc/66", request.getEndpoint());
    }

    @Test
    void shouldSkipUpsertWhenAfterIsNull() throws Exception {
        DebeziumEnvelope event = new DebeziumEnvelope();
        event.setOp("u");

        projector.apply("syncstream.public.products", "{\"id\":100}", event);

        verify(client, never()).performRequest(any(Request.class));
    }

    private StatusLine statusLine(int code) {
        return new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), code, "status");
    }
}
