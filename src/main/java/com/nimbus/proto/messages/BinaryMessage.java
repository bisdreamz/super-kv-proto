package com.nimbus.proto.messages;


import com.nimbus.proto.protocol.HeaderProtocol;
import io.netty.buffer.*;

public abstract class BinaryMessage {

    public static final ByteBufAllocator alloc = PooledByteBufAllocator.DEFAULT;

    protected ByteBuf buffer;
    final int startOfData;

    public BinaryMessage(ByteBuf buffer, int startOfData) {
        this.buffer = buffer;
        this.startOfData = startOfData;
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

    public byte[] key() {
        int len = HeaderProtocol.readInt(buffer, HeaderProtocol.SZ_KEY_LEN);

        byte[] key = HeaderProtocol.readBytes(buffer, len);

        return key;
    }

    public void key(byte[] key) {
        ensureCapacity(HeaderProtocol.SZ_KEY_LEN + key.length);

        HeaderProtocol.writeInt(buffer, HeaderProtocol.SZ_KEY_LEN, key.length);
        HeaderProtocol.writeBytes(buffer, key);
    }

    public byte[] value() {
        int len = HeaderProtocol.readInt(buffer, HeaderProtocol.SZ_VALUE_LEN);
        byte[] value = HeaderProtocol.readBytes(buffer, len);
        return value;
    }

    public void value(byte[] value) {
        ensureCapacity(HeaderProtocol.SZ_VALUE_LEN + value.length);

        HeaderProtocol.writeInt(buffer, HeaderProtocol.SZ_VALUE_LEN, value.length);
        HeaderProtocol.writeBytes(buffer, value);
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

    public void printDebug() {
        System.out.println(ByteBufUtil.hexDump(buffer));
    }
}