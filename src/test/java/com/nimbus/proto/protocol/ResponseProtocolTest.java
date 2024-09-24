package com.nimbus.proto.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ResponseProtocolTest {

    private ByteBuf buffer;

    @BeforeEach
    void setup() {
        // Initialize the buffer with enough space for header and some payload
        buffer = Unpooled.buffer(256);
    }

    @Test
    void testWriteAndReadStatus() {
        // Step 1: Write the status, compression, and count into the response header
        HeaderProtocol.setCompression(buffer, 0); // No compression
        ResponseProtocol.setStatus(buffer, ResponseProtocol.STATUS_OK); // Set status to OK
        HeaderProtocol.setCount(buffer, 2); // Indicate 2 key/value pairs

        // Step 2: Validate the written header fields
        assertEquals(ResponseProtocol.STATUS_OK, ResponseProtocol.getStatus(buffer), "Status mismatch");
        assertEquals(0, HeaderProtocol.getCompression(buffer), "Compression mismatch");
        assertEquals(2, HeaderProtocol.getCount(buffer), "Count mismatch");

        // Step 3: Write key/value data after the header
        buffer.writerIndex(ResponseProtocol.START_OF_DATA); // Set writer index to start of payload

        String key1 = "key1";
        String value1 = "value1";

        HeaderProtocol.writeInt(buffer, HeaderProtocol.SZ_KEY_LEN, key1.length()); // Write key length
        HeaderProtocol.writeBytes(buffer, key1.getBytes()); // Write key

        HeaderProtocol.writeInt(buffer, HeaderProtocol.SZ_VALUE_LEN, value1.length()); // Write value length
        HeaderProtocol.writeBytes(buffer, value1.getBytes()); // Write value

        // Ensure the writer index moved correctly
        assertEquals(ResponseProtocol.START_OF_DATA + HeaderProtocol.SZ_KEY_LEN + key1.length() + HeaderProtocol.SZ_VALUE_LEN + value1.length(),
                buffer.writerIndex(), "Writer index mismatch after writing key1/value1");

        // Step 4: Reset reader index to start of payload and read back key/value
        buffer.readerIndex(ResponseProtocol.START_OF_DATA); // Set reader index to start of payload

        int key1Len = HeaderProtocol.readInt(buffer, HeaderProtocol.SZ_KEY_LEN); // Read key length
        byte[] key1Bytes = HeaderProtocol.readBytes(buffer, key1Len); // Read key
        assertEquals(key1, new String(key1Bytes), "Key1 mismatch");

        int value1Len = HeaderProtocol.readInt(buffer, HeaderProtocol.SZ_VALUE_LEN); // Read value length
        byte[] value1Bytes = HeaderProtocol.readBytes(buffer, value1Len); // Read value
        assertEquals(value1, new String(value1Bytes), "Value1 mismatch");

        // Ensure reader index is at the correct position after reading key1/value1
        assertEquals(ResponseProtocol.START_OF_DATA + HeaderProtocol.SZ_KEY_LEN + key1.length() + HeaderProtocol.SZ_VALUE_LEN + value1.length(),
                buffer.readerIndex(), "Reader index mismatch after reading key1/value1");
    }

    @Test
    void testMultipleKeyValuePairsInResponse() {
        buffer.writerIndex(ResponseProtocol.START_OF_DATA); // Set writer index to start of payload

        // Write key1/value1
        String key1 = "key1";
        String value1 = "value1";
        HeaderProtocol.writeInt(buffer, HeaderProtocol.SZ_KEY_LEN, key1.length()); // Write key1 length
        HeaderProtocol.writeBytes(buffer, key1.getBytes()); // Write key1
        HeaderProtocol.writeInt(buffer, HeaderProtocol.SZ_VALUE_LEN, value1.length()); // Write value1 length
        HeaderProtocol.writeBytes(buffer, value1.getBytes()); // Write value1

        // Write key2/value2
        String key2 = "key2";
        String value2 = "value2";
        HeaderProtocol.writeInt(buffer, HeaderProtocol.SZ_KEY_LEN, key2.length()); // Write key2 length
        HeaderProtocol.writeBytes(buffer, key2.getBytes()); // Write key2
        HeaderProtocol.writeInt(buffer, HeaderProtocol.SZ_VALUE_LEN, value2.length()); // Write value2 length
        HeaderProtocol.writeBytes(buffer, value2.getBytes()); // Write value2

        // Step 3: Read back both key/value pairs
        buffer.readerIndex(ResponseProtocol.START_OF_DATA); // Set reader index to start of payload

        // Read key1/value1
        int key1Len = HeaderProtocol.readInt(buffer, HeaderProtocol.SZ_KEY_LEN); // Read key1 length
        byte[] key1Bytes = HeaderProtocol.readBytes(buffer, key1Len); // Read key1
        assertEquals(key1, new String(key1Bytes), "Key1 mismatch");

        int value1Len = HeaderProtocol.readInt(buffer, HeaderProtocol.SZ_VALUE_LEN); // Read value1 length
        byte[] value1Bytes = HeaderProtocol.readBytes(buffer, value1Len); // Read value1
        assertEquals(value1, new String(value1Bytes), "Value1 mismatch");

        // Read key2/value2
        int key2Len = HeaderProtocol.readInt(buffer, HeaderProtocol.SZ_KEY_LEN); // Read key2 length
        byte[] key2Bytes = HeaderProtocol.readBytes(buffer, key2Len); // Read key2
        assertEquals(key2, new String(key2Bytes), "Key2 mismatch");

        int value2Len = HeaderProtocol.readInt(buffer, HeaderProtocol.SZ_VALUE_LEN); // Read value2 length
        byte[] value2Bytes = HeaderProtocol.readBytes(buffer, value2Len); // Read value2
        assertEquals(value2, new String(value2Bytes), "Value2 mismatch");

        ResponseProtocol.setStatus(buffer, ResponseProtocol.STATUS_OK); // Set status to OK
        HeaderProtocol.setCompression(buffer, 0); // No compression
        HeaderProtocol.setCount(buffer, 2); // Indicate 2 key/value pairs
    }
}