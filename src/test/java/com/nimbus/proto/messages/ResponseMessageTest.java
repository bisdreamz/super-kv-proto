package com.nimbus.proto.messages;

import com.nimbus.proto.protocol.RequestProtocol;
import com.nimbus.proto.protocol.ResponseProtocol;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ResponseMessageTest {

    private ByteBuf buffer;

    @BeforeEach
    void setup() {
        buffer = PooledByteBufAllocator.DEFAULT.buffer(64);
    }

    @Test
    void testReuseBufferFromRequest() {
        // Prepare the request buffer with a SET command and a single key-value pair
        RequestMessage reqMsg = new RequestMessage(buffer);
        reqMsg.command(RequestProtocol.CMD_SET);
        reqMsg.compression(0);
        reqMsg.count(1);

        // Add key and value to the request
        byte[] key = "key1".getBytes();
        byte[] value = "value1".getBytes();
        reqMsg.key(key);
        reqMsg.value(value);

        // Now reuse the buffer in the response
        ResponseMessage resMsg = new ResponseMessage(reqMsg);
        resMsg.status(ResponseProtocol.STATUS_OK);
        resMsg.count(1);

        // Verify that the response is correctly using the same buffer and the new data is correct
        assertEquals(ResponseProtocol.STATUS_OK, resMsg.status());
        assertEquals(1, resMsg.count());

        // Print debug info to check the buffer
        resMsg.printDebug();
    }

    @Test
    void testOfMethodReuseBuffer() {
        RequestMessage reqMsg = new RequestMessage(buffer);
        ResponseMessage resMsg = ResponseMessage.of(reqMsg.buffer(), ResponseProtocol.STATUS_OK, 2);
        assertEquals(ResponseProtocol.STATUS_OK, resMsg.status());
        assertEquals(2, resMsg.count());
    }
}