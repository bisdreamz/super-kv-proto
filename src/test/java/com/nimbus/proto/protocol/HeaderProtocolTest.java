package com.nimbus.proto.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class HeaderProtocolTest {

    private ByteBuf buffer;

    @BeforeEach
    public void setup() {
        buffer = Unpooled.buffer(32);  // Creating a buffer of size 32 bytes for testing
    }

    @Test
    public void testSetAndGetMajor() {
        int majorValue = RequestProtocol.CMD_GET;

        HeaderProtocol.setMajor(buffer, majorValue);
        int result = HeaderProtocol.getMajor(buffer);

        assertEquals(majorValue, result, "Major value should match the written value");
    }

    @Test
    public void testSetAndGetCompression() {
        int compressionValue = 0x01;

        HeaderProtocol.setCompression(buffer, compressionValue);
        int result = HeaderProtocol.getCompression(buffer);

        assertEquals(compressionValue, result, "Compression value should match the written value");
    }

    @Test
    public void testSetAndGetCount() {
        int countValue = 0x03;

        HeaderProtocol.setCount(buffer, countValue);
        int result = HeaderProtocol.getCount(buffer);

        assertEquals(countValue, result, "Count value should match the written value");
    }

    @Test
    public void testReadAndWriteNumber() {
        int intValue = 123456789;

        HeaderProtocol.writeNumber(buffer, 4, intValue);
        buffer.readerIndex(0);  // Reset the reader index for reading
        int result = (int) HeaderProtocol.readNumber(buffer, 4);

        assertEquals(intValue, result, "Int value should match the written value");
    }

    @Test
    public void testSetAndGetNumber() {
        int intValue = 255;  // Example value
        int offset = 5;  // Writing at an offset of 5 bytes

        HeaderProtocol.setInt(buffer, offset, Integer.BYTES, intValue);
        int result = (int) HeaderProtocol.getNumber(buffer, offset, Integer.BYTES);

        assertEquals(intValue, result, "Value read from buffer should match the written value");
    }

    @Test
    public void testReadAndWriteBytes() {
        byte[] data = "TestData".getBytes();

        HeaderProtocol.writeBytes(buffer, data);
        buffer.readerIndex(0); // Reset reader index to read from the start
        byte[] result = HeaderProtocol.readBytes(buffer, data.length);

        assertArrayEquals(data, result, "Byte arrays should match");
    }

    @Test
    public void testSetAndGetBytes() {
        byte[] data = "Auth".getBytes();
        int offset = 10; // Write starting from byte 10

        HeaderProtocol.setBytes(buffer, offset, data);
        byte[] result = HeaderProtocol.getBytes(buffer, offset, data.length);

        assertArrayEquals(data, result, "Byte arrays should match the written bytes");
    }

    @Test
    public void testSetAndGetAuth() {
        byte[] authData = "1234".getBytes(); // Example auth data (size of MAX_AUTH_LEN)

        HeaderProtocol.setAuth(buffer, authData);
        byte[] result = HeaderProtocol.getAuth(buffer);

        assertArrayEquals(authData, result, "Auth data should match the written value");
    }
}