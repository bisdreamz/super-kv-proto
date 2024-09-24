package com.nimbus.proto.messages;

import com.nimbus.proto.protocol.HeaderProtocol;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


import static org.junit.jupiter.api.Assertions.*;

class BinaryMessageTest {

    private ByteBuf buffer;

    @BeforeEach
    void setup() {
        buffer = PooledByteBufAllocator.DEFAULT.buffer(64);
    }

    @Test
    void testKeyAndValueWriteRead() {
        BinaryMessage message = new BinaryMessage(buffer, HeaderProtocol.HDR_END_OFFSET) {};

        byte[] key = "key1".getBytes();
        byte[] value = "value1".getBytes();

        message.resetWriteIndex();

        message.key(key);
        message.value(value);

        message.resetReadIndex();

        assertArrayEquals(key, message.key());
        assertArrayEquals(value, message.value());
    }

    @Test
    void testEnsureCapacity() {
        BinaryMessage message = new BinaryMessage(buffer, HeaderProtocol.HDR_END_OFFSET) {};

        byte[] largeKey = new byte[128];
        message.key(largeKey);

        assertTrue(message.buffer().capacity() >= 128, "Expected capacity to be at least 128 bytes found "
                + buffer.capacity());
    }

}