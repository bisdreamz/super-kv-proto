package com.nimbus.proto.messages;


import com.nimbus.proto.protocol.HeaderProtocol;
import io.netty.buffer.*;

public abstract class BinaryMessage {

    public static final ByteBufAllocator alloc = PooledByteBufAllocator.DEFAULT;

    private static final byte[] EMPTY_BYTES = new byte[0];

    protected ByteBuf buffer;
    final int startOfData;

    public BinaryMessage(ByteBuf buffer, int startOfData) {
        this.buffer = buffer;
        this.startOfData = startOfData;
    }

    /**
     * Release the underlying byte buffer.
     * Must always be called, and only after all use of buffer is completed
     */
    public void release() {
        buffer.release();
    }
    /**
     * Required to reset writeIndex on byffer
     * after you write a fixed location field
     * such as command or status
     */
    void resetWriteIndex() {
        buffer.writerIndex(this.startOfData);
    }

    /**
     * Required to reset readIndex on byffer
     * after you read a fixed location field
     * such as command or status
     */
    void resetReadIndex() {
        buffer.readerIndex(this.startOfData);
    }

    private static int alignToBytes(int requiredCapacity, int alignment) {
        return (requiredCapacity + (alignment - 1)) & ~(alignment - 1);
    }

    /**
     * Resets the reader index to the start of all key or value data,
     * so they may be read again from the underlying buffer
     */
    public void resetReaderIndex() {
        buffer.readerIndex(startOfData);
    }

    /**
     * Expands the provided buffer if needed to accommodate a potential write.
     * @implNote First expansion copies the original buffer, subsequent resizes
     * expand a composite buf. At time of writing, appears to be a bug in composite
     * buffers if we use the original buffer.
     * @param requiredCapacity required write capacity, which triggers an expansion if
     *                         requiredCapacity > writeableBytes
     */
    void ensureCapacity(int requiredCapacity) {
        if (buffer.writableBytes() < requiredCapacity) {
            int currCapacity = buffer.capacity();
            int minWritableBytes = requiredCapacity - buffer.writableBytes();
            int capacityIncrement = alignToBytes(minWritableBytes, 64);

            // Store current state
            int readerIndex = buffer.readerIndex();
            int writerIndex = buffer.writerIndex();

            if (buffer instanceof CompositeByteBuf) {
                CompositeByteBuf compositeBuffer = (CompositeByteBuf) buffer;
                ByteBuf additionalBuffer = alloc.directBuffer(capacityIncrement);
                compositeBuffer.addComponent(true, additionalBuffer);
                compositeBuffer.writerIndex(writerIndex);
            } else {
                // Create new composite buffer
                CompositeByteBuf newCompositeBuffer = alloc.compositeBuffer();
                ByteBuf additionalBuffer = alloc.directBuffer(capacityIncrement);

                // Copy original buffer once
                ByteBuf copiedContent = alloc.directBuffer(currCapacity);
                buffer.getBytes(0, copiedContent, writerIndex);

                // Add both buffers and set indices
                newCompositeBuffer.addComponents(true, copiedContent, additionalBuffer);
                newCompositeBuffer.writerIndex(writerIndex);
                newCompositeBuffer.readerIndex(readerIndex);

                // Switch buffers
                ByteBuf oldBuffer = buffer;
                buffer = newCompositeBuffer;
                oldBuffer.release();
            }

            System.out.println("Buffer capacity increased to: " + buffer.capacity() +
                    ", writable bytes: " + buffer.writableBytes());
        }
    }

    public byte[] keyAsBytes() {
        int len = this.dataLen(HeaderProtocol.SZ_KEY_LEN);
        if (len < 1)
            throw new IllegalStateException("Error reading key with zero length, got " + len);

        if (buffer.readableBytes() < len) {
            throw new IllegalStateException("Less readable bytes " + buffer.readableBytes() +
                    " in buffer than indicated length in value data " + len);
        }

        byte[] key = HeaderProtocol.readBytes(buffer, len);

        return key;
    }

    public int keyAsInt() {
        this.readVerifyDataLen(HeaderProtocol.SZ_KEY_LEN, Integer.BYTES);

        return (int) HeaderProtocol.readNumber(buffer, Integer.BYTES);
    }

    public int keyAsLong() {
        this.readVerifyDataLen(HeaderProtocol.SZ_KEY_LEN, Long.BYTES);

        return (int) HeaderProtocol.readNumber(buffer, Long.BYTES);
    }

    public String keyAsString() {
        return new String(this.keyAsBytes());
    }

    /**
     * Convenience method to accept key of variable types
     * @param key of type byte[], String, Integer, or Long
     * @throws IllegalArgumentException if provided an unsupported key type
     */
    public void key(Object key) {
        if (key == null)
            throw new NullPointerException("Key value cannot be null!");

        if (key instanceof byte[]) {
            this.key((byte[]) key);
        } else if (key instanceof String) {
            this.key((String) key);
        } else if (key instanceof Integer) {
            this.key((Integer) key);
        } else if (key instanceof Long) {
            this.key((Long) key);
        } else {
            throw new IllegalArgumentException("Unsupported key type: " + key.getClass().getName());
        }
    }

    public void key(byte[] key) {
        if (key == null)
            throw new NullPointerException("Key must be non-null with non zero length");

        if (key.length == 0)
            throw new IllegalArgumentException("Key must be non-zero length");

        ensureCapacity(HeaderProtocol.SZ_KEY_LEN + key.length);

        HeaderProtocol.writeNumber(buffer, HeaderProtocol.SZ_KEY_LEN, key.length);
        HeaderProtocol.writeBytes(buffer, key);
    }

    private void key(long value, int sz) {
        ensureCapacity(HeaderProtocol.SZ_KEY_LEN + sz);

        HeaderProtocol.writeNumber(buffer, HeaderProtocol.SZ_KEY_LEN, sz);
        HeaderProtocol.writeNumber(buffer, sz, value);
    }

    public void key(int key) {
        this.key(key, Integer.BYTES);
    }

    public void key(long key) {
        this.key(key, Long.BYTES);
    }

    public void key(String key) {
        if (key == null)
            throw new NullPointerException("Key must be non-null with non zero length");

        if (key.length() == 0)
            throw new IllegalArgumentException("Key must be non-zero length");

        this.key(key.getBytes());
    }

    /**
     * Read the length entry of a key or value header
     * which directly precedes the key or value data.
     * @param szBytes bytes of header length field to read, not the size of the value
     * @return the integer value read, indicating the length in bytes of the key or value data
     */
    public int dataLen(int szBytes) {
        if (buffer.readerIndex() < startOfData)
            buffer.readerIndex(startOfData);

        int len = (int) HeaderProtocol.readNumber(buffer, szBytes);

        if (len < 0)
            throw new IllegalStateException("Length field cannot have a negative value");

        return len;
    }

    /**
     * Verifies given key or value field is of the expected size for constant data types
     * @param szBytes size in bytes of the key of value length entry in the header, e.g. 4 bytes if an integer
     *                entry indicates the total size of the following data
     * @param expectedValue the expected value of the bytes read e.g. if the value is expected to be a Long
     *                      then this value should be 8
     * @implNote This verifies expected data sizes and will advance the reader index as required
     */
    public void readVerifyDataLen(int szBytes, int expectedValue) {
        int len = this.dataLen(szBytes);

        if (len != expectedValue)
            throw new IllegalStateException("Key or Value length field does not match expected value");
    }

    public byte[] valueAsBytes() {
        int len = this.dataLen(HeaderProtocol.SZ_VALUE_LEN);

        if (len < 0)
            throw new IllegalStateException("Value length field cannot have be negative");

        if (len == 0) // item present but empty
            return EMPTY_BYTES;

        if (buffer.readableBytes() < len)
            throw new IllegalStateException("Less readable bytes in buffer than indicated length in value data");

        return HeaderProtocol.readBytes(buffer, len);
    }

    public String valueAsString() {
        byte[] value = this.valueAsBytes();

        return value != null ? new String(value) : null;
    }

    public byte valueAsByte() {
        this.readVerifyDataLen(HeaderProtocol.SZ_VALUE_LEN, Byte.BYTES);

        return (byte) HeaderProtocol.readNumber(buffer, Byte.BYTES);
    }

    public short valueAsShort() {
        this.readVerifyDataLen(HeaderProtocol.SZ_VALUE_LEN, Short.BYTES);

        return (short) HeaderProtocol.readNumber(buffer, Short.BYTES);
    }

    public int valueAsInt() {
        this.readVerifyDataLen(HeaderProtocol.SZ_VALUE_LEN, Integer.BYTES);

        return (int) HeaderProtocol.readNumber(buffer, Integer.BYTES);
    }

    public long valueAsLong() {
        this.readVerifyDataLen(HeaderProtocol.SZ_VALUE_LEN, Long.BYTES);

        return (long) HeaderProtocol.readNumber(buffer, Long.BYTES);
    }

    public boolean valueAsBoolean() {
        return valueAsByte() != 0;
    }

    public void value(Object value) {
        if (value == null)
            throw new NullPointerException("Value must not be null");

        if (value instanceof byte[]) {
            this.value((byte[]) value);
        } else if (value instanceof String) {
            this.value((String) value);
        } else if (value instanceof Byte) {
            this.value((byte) value);
        } else if (value instanceof Short) {
            this.value((short) value);
        } else if (value instanceof Integer) {
            this.value((Integer) value);
        } else if (value instanceof Long) {
            this.value((Long) value);
        } else if (value instanceof Boolean) {
            this.value((Boolean) value);
        } else {
            throw new IllegalArgumentException("Unsupported value type: " + value.getClass().getName());
        }
    }

    public void value(String value) {
        if (value == null)
            throw new NullPointerException("Value cannot be null");

        this.value(value.getBytes());
    }

    public void value(byte[] value) {
        ensureCapacity(HeaderProtocol.SZ_VALUE_LEN + value.length);

        HeaderProtocol.writeNumber(buffer, HeaderProtocol.SZ_VALUE_LEN, value.length);
        HeaderProtocol.writeBytes(buffer, value);
    }

    public void value(long value, int sz) {
        ensureCapacity(HeaderProtocol.SZ_VALUE_LEN + sz);

        HeaderProtocol.writeNumber(buffer, HeaderProtocol.SZ_VALUE_LEN, sz);
        HeaderProtocol.writeNumber(buffer, sz, value);
    }

    public void value(Boolean value) {
        this.value(value ? (byte) 1 : (byte) 0, Byte.BYTES);
    }

    public void value(Byte value) {
        this.value(value, Byte.BYTES);
    }

    public void value(Short value) {
        this.value(value, Short.BYTES);
    }

    public void value(Integer value) {
        this.value(value, Integer.BYTES);
    }

    public void value(Long value) {
        this.value(value, Long.BYTES);
    }

    public ByteBuf end() {
        buffer.readerIndex(0);

        // Ensure we always write the full header
        // even if no payload data
        if (buffer.writerIndex() < startOfData)
            this.resetWriteIndex();

        HeaderProtocol.setTotalLen(buffer);

        return buffer;
    }

    public ByteBuf buffer() {
        return buffer;
    }

    public int totalLength() {
        return HeaderProtocol.getTotalLen(buffer);
    }

    public void printDebug() {
        System.out.println(ByteBufUtil.hexDump(buffer));
    }
}