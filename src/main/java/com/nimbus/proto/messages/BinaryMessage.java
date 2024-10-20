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

    public static void main(String... a) {
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.buffer(64);
        byte[] bytes = new byte[128];
        BinaryMessage msg = new BinaryMessage(buffer, 16) {};
        msg.key(bytes);
    }

    void ensureCapacity(int requiredCapacity) {
        if (buffer.writableBytes() < requiredCapacity) {
            int currCapacity = buffer.capacity();
            int currCapacityRemaining = buffer.writableBytes();
            int reqCapacityMissing = requiredCapacity - currCapacityRemaining;
            int totalCapacityNeeded = currCapacity + reqCapacityMissing;
            int newTotalCapacity = alignToBytes(totalCapacityNeeded, 64);
            int neededAllocation = newTotalCapacity - currCapacity;

            ByteBuf additionalBuffer = alloc.buffer(neededAllocation);

            if (buffer instanceof CompositeByteBuf) {
                CompositeByteBuf compositeBuffer = (CompositeByteBuf) buffer;
                compositeBuffer.addComponent(additionalBuffer);
                compositeBuffer.capacity(newTotalCapacity);
            } else {
                // Create a CompositeByteBuf and add both the existing buffer and the new buffer
                CompositeByteBuf compositeBuffer = alloc.compositeBuffer();

                compositeBuffer.addComponent(buffer.retain());
                compositeBuffer.addComponent(additionalBuffer);
                compositeBuffer.capacity(newTotalCapacity);

                compositeBuffer.setIndex(buffer.readerIndex(), buffer.writerIndex());

                buffer = compositeBuffer;
            }
        }
    }

    public byte[] keyAsBytes() {
        int len = this.dataLen(HeaderProtocol.SZ_KEY_LEN);
        if (len < 1)
            throw new IllegalStateException("Error reading key with zero length, got " + len);

        if (buffer.readableBytes() < len)
            throw new IllegalStateException("Less readable bytes in buffer than indicated length in value data");

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

    public void key(byte[] key) {
        if (key == null || key.length < 1)
            throw new IllegalStateException("Key must be non-null with non zero length");

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
        if (key == null || key.isEmpty())
            throw new IllegalStateException("Key must be non-null with non zero length");

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

    public void value(String value) {
        if (value == null)
            throw new IllegalArgumentException("Value cannot be null");

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

    public void value(boolean value) {
        this.value(value ? (byte) 1 : (byte) 0, Byte.BYTES);
    }

    public void value(byte value) {
        this.value(value, Byte.BYTES);
    }

    public void value(short value) {
        this.value(value, Short.BYTES);
    }

    public void value(int value) {
        this.value(value, Integer.BYTES);
    }

    public void value(long value) {
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