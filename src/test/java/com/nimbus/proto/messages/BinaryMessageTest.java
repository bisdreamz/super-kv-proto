package com.nimbus.proto.messages;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.jupiter.api.*;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.*;

public class BinaryMessageTest {

    private static final ByteBufAllocator ALLOCATOR = PooledByteBufAllocator.DEFAULT;
    private ByteBuf buffer;
    private BinaryMessage binaryMessage;

    @BeforeEach
    public void setUp() {
        // Allocate a buffer with initial capacity
        buffer = ALLOCATOR.buffer(64);
        // Set startOfData to match the initial writer index
        binaryMessage = new BinaryMessage(buffer, buffer.writerIndex()) {};
    }

    @Test
    public void fuuuuuuuck() {
        ResponseMessage responseMessage = new ResponseMessage();

        byte[] keyb = new byte[32];
        byte[] valb = new byte[32];
        ThreadLocalRandom.current().nextBytes(keyb);
        ThreadLocalRandom.current().nextBytes(valb);

        String key = Arrays.toString(keyb);
        String val = Arrays.toString(valb);
        responseMessage.key(key);
        responseMessage.value(val);

        responseMessage.end();
        responseMessage.resetReaderIndex();

        assertEquals(responseMessage.keyAsString(), key, "Key should match after expansion");
        assertEquals(responseMessage.valueAsString(), val, "Value should match after expansion");
    }

    @AfterEach
    public void tearDown() {
        // Release the buffer to prevent memory leaks
        if (buffer.refCnt() > 0) {
            buffer.release();
        }
    }

    @Test
    public void testKeyAndValueMethods() {
        // Test writing and reading a key and value
        String testKey = "testKey";
        String testValue = "testValue";

        binaryMessage.key(testKey);
        binaryMessage.value(testValue);

        // Reset reader index to start reading
        binaryMessage.resetReaderIndex();

        // Read and verify key
        String readKey = binaryMessage.keyAsString();
        assertEquals(testKey, readKey, "The read key should match the written key");

        // Read and verify value
        String readValue = binaryMessage.valueAsString();
        assertEquals(testValue, readValue, "The read value should match the written value");
    }

    @Test
    public void testEnsureCapacityDataPreservation() {
        // Initial data to write
        String initialData = "InitialData";
        binaryMessage.value(initialData);

        // Calculate remaining writable bytes
        int writableBytes = buffer.writableBytes();

        // Data to cause buffer expansion
        byte[] expansionData = new byte[writableBytes + 10]; // Exceeds current capacity
        for (int i = 0; i < expansionData.length; i++) {
            expansionData[i] = (byte) (i % 256);
        }

        // Write data to trigger buffer expansion
        binaryMessage.value(expansionData);

        // Reset reader index to start of data
        binaryMessage.resetReaderIndex();

        // Read initial data
        String readInitialData = binaryMessage.valueAsString();
        assertEquals(initialData, readInitialData, "Initial data should be preserved after buffer expansion");

        // Read expanded data
        byte[] readExpansionData = binaryMessage.valueAsBytes();
        assertArrayEquals(expansionData, readExpansionData, "Expansion data should be correctly written and read after buffer expansion");
    }

    @Test
    public void testValueAsInt() {
        int testValue = 123456789;
        binaryMessage.value(testValue);

        // Reset reader index
        binaryMessage.resetReaderIndex();

        int readValue = binaryMessage.valueAsInt();
        assertEquals(testValue, readValue, "The read integer value should match the written value");
    }

    @Test
    public void testValueAsLong() {
        long testValue = 9876543210L;
        binaryMessage.value(testValue);

        // Reset reader index
        binaryMessage.resetReaderIndex();

        long readValue = binaryMessage.valueAsLong();
        assertEquals(testValue, readValue, "The read long value should match the written value");
    }

    @Test
    public void testValueAsByte() {
        byte testValue = (byte) 123;
        binaryMessage.value(testValue);

        // Reset reader index
        binaryMessage.resetReaderIndex();

        byte readValue = binaryMessage.valueAsByte();
        assertEquals(testValue, readValue, "The read byte value should match the written value");
    }

    @Test
    public void testValueAsShort() {
        short testValue = (short) 32000;
        binaryMessage.value(testValue);

        // Reset reader index
        binaryMessage.resetReaderIndex();

        short readValue = binaryMessage.valueAsShort();
        assertEquals(testValue, readValue, "The read short value should match the written value");
    }

    @Test
    public void testValueAsBoolean() {
        boolean testValue = true;
        binaryMessage.value(testValue);

        // Reset reader index
        binaryMessage.resetReaderIndex();

        boolean readValue = binaryMessage.valueAsBoolean();
        assertEquals(testValue, readValue, "The read boolean value should match the written value");
    }

    @Test
    public void testMultipleValuesWithExpansion() {
        // Write multiple values to trigger multiple expansions
        String value1 = "FirstValue";
        String value2 = "SecondValueThatIsLonger";
        String value3 = "ThirdValueThatIsEvenLongerThanSecondValue";

        binaryMessage.value(value1);
        binaryMessage.value(value2);
        binaryMessage.value(value3);

        // Reset reader index
        binaryMessage.resetReaderIndex();

        // Read and verify values
        String readValue1 = binaryMessage.valueAsString();
        assertEquals(value1, readValue1, "First value should match");

        String readValue2 = binaryMessage.valueAsString();
        assertEquals(value2, readValue2, "Second value should match");

        String readValue3 = binaryMessage.valueAsString();
        assertEquals(value3, readValue3, "Third value should match");
    }

    @Test
    public void testDataIntegrityAfterMultipleExpansions() {
        // Write initial data
        String initialData = "InitialData";
        binaryMessage.value(initialData);

        // Write data to trigger first expansion
        byte[] data1 = new byte[buffer.writableBytes() + 50];
        for (int i = 0; i < data1.length; i++) {
            data1[i] = (byte) (i % 256);
        }
        binaryMessage.value(data1);

        // Write data to trigger second expansion
        byte[] data2 = new byte[buffer.writableBytes() + 100];
        for (int i = 0; i < data2.length; i++) {
            data2[i] = (byte) ((i + 100) % 256);
        }
        binaryMessage.value(data2);

        // Reset reader index
        binaryMessage.resetReaderIndex();

        // Read and verify initial data
        String readInitialData = binaryMessage.valueAsString();
        assertEquals(initialData, readInitialData, "Initial data should be preserved after multiple buffer expansions");

        // Read and verify first expansion data
        byte[] readData1 = binaryMessage.valueAsBytes();
        assertArrayEquals(data1, readData1, "First expansion data should match");

        // Read and verify second expansion data
        byte[] readData2 = binaryMessage.valueAsBytes();
        assertArrayEquals(data2, readData2, "Second expansion data should match");
    }

    @Test
    public void testExceptionOnNullKey() {
        Exception exception = assertThrows(NullPointerException.class, () -> {
            binaryMessage.key((String) null);
        });
    }

    @Test
    public void testExceptionOnNullValue() {
        Exception exception = assertThrows(NullPointerException.class, () -> {
            binaryMessage.value((String) null);
        });
    }

    @Test
    public void testUnsupportedKeyType() {
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            binaryMessage.key(3.14); // Double is not supported
        });
        assertTrue(exception.getMessage().contains("Unsupported key type"));
    }

    @Test
    public void testUnsupportedValueType() {
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            binaryMessage.value(3.14); // Double is not supported
        });
        assertTrue(exception.getMessage().contains("Unsupported value type"));
    }

    @Test
    public void testKeyAsIntWithInvalidLength() {
        // Manually write incorrect length
        buffer.writeInt(2); // Incorrect length for an int (should be 4)
        buffer.writeShort(12345); // Write 2 bytes instead of 4

        binaryMessage.resetReaderIndex();

        Exception exception = assertThrows(IllegalStateException.class, () -> {
            binaryMessage.keyAsInt();
        });
        assertEquals("Key or Value length field does not match expected value", exception.getMessage());
    }

    @Test
    public void testValueAsLongWithInvalidLength() {
        // Manually write incorrect length
        buffer.writeInt(4); // Incorrect length for a long (should be 8)
        buffer.writeInt(123456789); // Write 4 bytes instead of 8

        binaryMessage.resetReaderIndex();

        Exception exception = assertThrows(IllegalStateException.class, () -> {
            binaryMessage.valueAsLong();
        });
        assertEquals("Key or Value length field does not match expected value", exception.getMessage());
    }
}