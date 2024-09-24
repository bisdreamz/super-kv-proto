package com.nimbus.proto.messages;

import com.nimbus.proto.protocol.RequestProtocol;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RequestMessageTest {

    private ByteBuf buffer;

    @BeforeEach
    void setup() {
        buffer = PooledByteBufAllocator.DEFAULT.buffer(64);
    }

    @Test
    void testWriteAndReadCommand() {
        RequestMessage msg = new RequestMessage(buffer);
        msg.command(RequestProtocol.CMD_GET);
        assertEquals(RequestProtocol.CMD_GET, msg.command());
    }

    @Test
    void testWriteAndReadCompression() {
        RequestMessage msg = new RequestMessage(buffer);
        msg.compression(1);
        assertEquals(1, msg.compression());
    }

    @Test
    void testWriteAndReadCount() {
        RequestMessage msg = new RequestMessage(buffer);
        msg.count(5);
        assertEquals(5, msg.count());
    }

    @Test
    void testWriteAndReadCommandCompressionCount() {
        RequestMessage msg = new RequestMessage(buffer);
        msg.command(RequestProtocol.CMD_SET);
        msg.compression(1);
        msg.count(10);
        assertEquals(RequestProtocol.CMD_SET, msg.command());
        assertEquals(1, msg.compression());
        assertEquals(10, msg.count());
    }

    @Test
    void testWriteAndReadKeyValueAfterHeaderFields() {
        RequestMessage msg = new RequestMessage(buffer);
        msg.command(RequestProtocol.CMD_SET);
        msg.compression(0);
        msg.count(1);

        String key = "mykey";
        String value = "myvalue";
        msg.key(key.getBytes());
        msg.value(value.getBytes());

        msg.resetReadIndex();

        assertEquals(key, new String(msg.key()));
        assertEquals(value, new String(msg.value()));
    }
}