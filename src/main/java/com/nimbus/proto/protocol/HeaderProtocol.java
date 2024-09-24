package com.nimbus.proto.protocol;

import io.netty.buffer.ByteBuf;

public class HeaderProtocol {

    public static class HeaderEntry {
        private final int offset;
        private final int size;

        public HeaderEntry(int offset, int size) {
            this.offset = offset;
            this.size = size;
        }

        public int offsetStart() {
            return offset;
        }

        public int offsetEnd() {
            return offset + size;
        }

        public int sizeBytes() {
            return size;
        }
    }

    // DEFINITIONS OF HEADER VARIABLES
    /**
     * Max password length for auth, reserved in header
     */
    public static final int MAX_AUTH_LEN = 4;

    // DEFINITION OF FIELD HEADERS VALUES WHICH ARE DYNAMICALLY PLACED IN MESSAGES
    public static final int SZ_KEY_LEN = Short.BYTES;
    public static final int SZ_VALUE_LEN = Integer.BYTES;

    // DEFINITION OF SHARED PAYLOAD HEADER FIELDS IN THEIR FIXED ORDER AND OFFSETS
    public static final HeaderEntry HDR_TOTAL_LEN = new HeaderEntry(0, Integer.BYTES);
    public static final HeaderEntry HDR_MAJOR = new HeaderEntry(HDR_TOTAL_LEN.offsetEnd(), Byte.BYTES);
    public static final HeaderEntry HDR_COMPRESSION = new HeaderEntry(HDR_MAJOR.offsetEnd(), Byte.BYTES);
    public static final HeaderEntry HDR_COUNT = new HeaderEntry(HDR_COMPRESSION.offsetEnd(), Short.BYTES);
    public static final HeaderEntry HDR_AUTH = new HeaderEntry(HDR_COUNT.offsetEnd(), Byte.BYTES * MAX_AUTH_LEN);
    public static final HeaderEntry HDR_RESERVED = new HeaderEntry(HDR_AUTH.offsetEnd(), 16  - HDR_AUTH.offsetEnd());

    public static void preintDebugHeaderLayout() {
        System.out.print("Total Sz (" + HDR_TOTAL_LEN.sizeBytes() + " bytes) offsets "
                + HDR_TOTAL_LEN.offsetStart() + " -> " + HDR_TOTAL_LEN.offsetEnd() + "\t/\t");
        System.out.print("Major (" + HDR_MAJOR.sizeBytes() + " bytes) offsets "
                + HDR_MAJOR.offsetStart() + " -> " + HDR_MAJOR.offsetEnd() + "\t/\t");
        System.out.print("Compression (" + HDR_COMPRESSION.sizeBytes() + " bytes) offsets "
                + HDR_COMPRESSION.offsetStart() + " -> " + HDR_COMPRESSION.offsetEnd() + "\t/\t");
        System.out.print("Count (" + HDR_COUNT.sizeBytes() + " bytes) offsets "
                + HDR_COUNT.offsetStart() + " -> " + HDR_COUNT.offsetEnd() + "\t/\t");
        System.out.print("Auth (" + HDR_AUTH.sizeBytes() + " bytes) offsets "
                + HDR_AUTH.offsetStart() + " -> " + HDR_AUTH.offsetEnd() + "\t/\t");
        System.out.println("Reserved (" + HDR_RESERVED.sizeBytes() + " bytes) offsets "
                + HDR_RESERVED.offsetStart() + " -> " + HDR_RESERVED.offsetEnd());
    }

    /**
     * End offset and thus the size of the entire header object, where
     * buffer message data should begin.
     */
    public static final int HDR_END_OFFSET = HDR_RESERVED.offsetEnd();

    /**
     * Read a numerical value of the provided size from the current buffer
     * at readerIndex, and increment the readerIndex szBytes
     * @implNote Does modify the buffer reader index
     * @param buffer Buffer to read from
     * @param szBytes Size of numerical value
     * @return Number value read from current reader index of ByteBuf
     */
    public static int readInt(ByteBuf buffer, int szBytes) {
        return switch (szBytes) {
            case 1 -> buffer.readByte();
            case 2 -> buffer.readShort();
            case 4 -> buffer.readInt();
            default -> throw new IllegalArgumentException("Unhandled readInt of sz " + szBytes);
        };
    }

    /**
     * Get a numerical value of the provided size from the provided static offset,
     * <b>without</b> incrementing the readerIndex
     * @implNote Does not modify buffer reader index
     * @param buffer Buffer to read from
     * @param offset Static buffer offset to read from
     * @param szBytes Size of numerical value
     * @return Number value read from offset
     */
    public static int getInt(ByteBuf buffer, int offset, int szBytes) {
        return switch (szBytes) {
            case 1 -> buffer.getByte(offset);
            case 2 -> buffer.getShort(offset);
            case 4 -> buffer.getInt(offset);
            default -> throw new IllegalArgumentException("Unhandled getInt of sz " + szBytes);
        };
    }

    /**
     * Write a numerical value of the given size to the current writerIndex of the bytebuf,
     * which will increment the writer index of the buffer
     * @implNote Does modify buffer writer index
     * @param buffer ByteBuf to write to
     * @param szBytes Size of numerical value in bytes to write
     * @param value Value
     */
    public static void writeInt(ByteBuf buffer, int szBytes, int value) {
        switch (szBytes) {
            case 1 -> buffer.writeByte((byte) value);
            case 2 -> buffer.writeShort((short) value);
            case 4 -> buffer.writeInt(value);
            default -> throw new IllegalArgumentException("Unhandled writeInt of sz " + szBytes);
        }
    }

    /**
     * Set a numerical value of the given size to the provided static offset of the bytebuf,
     * which will <b>not</b> increment the writer index of the buffer
     * @implNote Does not modify buffer writer index
     * @param buffer ByteBuf to write to
     * @param szBytes Size of numerical value in bytes to write
     * @param value Value
     */
    public static void setInt(ByteBuf buffer,  int offset, int szBytes, int value) {
        switch (szBytes) {
            case 1 -> buffer.setByte(offset, (byte) value);
            case 2 -> buffer.setShort(offset, (short) value);
            case 4 -> buffer.setInt(offset, value);
            default -> throw new IllegalArgumentException("Unhandled writeInt of sz " + szBytes);
        }
    }

    /**
     * Read bytes from provided buffer starting from the current buffer
     * reader index, and incrementing the reader index in the process.
     * @implNote Does modify the buffer reader index
     * @param buffer Buffer to read from
     * @param length Number of bytes to read and increment reader index
     * @return byte array of data
     */
    public static byte[] readBytes(ByteBuf buffer, int length) {
        byte[] bytes = new byte[length];
        buffer.readBytes(bytes);
        return bytes;
    }

    /**
     * Get bytes from buffer starting from the provided static offset,
     * which will <b>not</b> increment the reader index in the process.
     * @implNote Does not modify buffer reader index
     * @param buffer Buffer to read from
     * @param offset Static offset into buffer to begin read from
     * @param length Number of bytes to read and increment reader index
     * @return byte array of data
     */
    public static byte[] getBytes(ByteBuf buffer, int offset, int length) {
        byte[] bytes = new byte[length];
        buffer.getBytes(offset, bytes);
        return bytes;
    }

    /**
     * Write bytes to the provided buffer starting from the current writer index
     * and incrementing the index in the process.
     * @implNote Does modify buffer writer index
     * @param buffer Buffer to write to
     * @param value Byte array value to write
     */
    public static void writeBytes(ByteBuf buffer, byte[] value) {
        buffer.writeBytes(value);
    }

    /**
     * Set bytes to the provided buffer starting from the given static offset,
     * which will <b>not</b> increment the writer index.
     * @implNote Does not modify buffer writer index
     * @param buffer Buffer to write to
     * @param offset Static offset into buffer to begin write
     * @param value Byte array to write
     */
    public static void setBytes(ByteBuf buffer, int offset, byte[] value) {
        buffer.setBytes(offset, value);
    }

    /**
     * Sets the total size header, which is the first 4 bytes of the message payload
     * based on the current buffer writer index, should be last operation called.
     * @see {@link #HDR_TOTAL_LEN}
     * @implNote Does not modify the buffer read or write index
     * @param buffer Buffer to write total length to
     */
    public static void setTotalLen(ByteBuf buffer) {
        setInt(buffer, HDR_TOTAL_LEN.offsetStart(), HDR_TOTAL_LEN.sizeBytes(), buffer.writerIndex());
    }

    /**
     * Get the total size of this message, including header and message.
     * @see {@link #HDR_TOTAL_LEN}
     * @implNote Does not modify the buffer read or write index
     * @param buffer
     * @return
     */
    public static int getTotalLen(ByteBuf buffer) {
        return getInt(buffer, HDR_TOTAL_LEN.offsetStart(), HDR_TOTAL_LEN.sizeBytes());
    }

    /**
     * Sets major byte {@link #HDR_MAJOR} of the protocol. For the request, this is often the command,
     * and response is the status. Does not increment buffer writer index.
     * @see {@link #HDR_MAJOR}
     * @implNote Does not modify the buffer read or write index
     * @param buffer Buffer to set value on
     * @param value value to set
     */
    public static void setMajor(ByteBuf buffer, int value) {
        setInt(buffer, HDR_MAJOR.offsetStart(), HDR_MAJOR.sizeBytes(), (byte) value);
    }

    /**
     * Gets major byte from buffer, often the command or response value.
     * @see {@link #HDR_MAJOR}
     * @implNote Does not modify the buffer read or write index
     * @param buffer Buffer to modify
     * @return The number value of the first major byte
     */
    public static int getMajor(ByteBuf buffer) {
        return getInt(buffer, HDR_MAJOR.offsetStart(), HDR_MAJOR.sizeBytes());
    }

    /**
     * Set the header flag {@link #HDR_COMPRESSION} indicating payload compression is present.
     * 1 indicates default compression method, higher values reserved for additional implementations.
     * @see {@link #HDR_COMPRESSION}
     * @implNote Does not modify the buffer read or write index
     * @param buffer Buffer to write to
     * @param compression Byte compression value indicator. Nonzero indicates compression.
     */
    public static void setCompression(ByteBuf buffer, int compression) {
        setInt(buffer, HDR_COMPRESSION.offsetStart(), HDR_COMPRESSION.sizeBytes(), compression);
    }

    /**
     * Get the compression header field numerical value. Non zero value
     * indicates some for of compression enabled.
     * @see {@link #HDR_COMPRESSION}
     * @implNote Does not modify the buffer read or write index
     * @param buffer Buffer to read from
     * @return Compression header value, specific value meaning differs by implementation
     */
    public static int getCompression(ByteBuf buffer) {
        return getInt(buffer, HDR_COMPRESSION.offsetStart(), HDR_COMPRESSION.sizeBytes());
    }

    /**
     * Get the generic count header value for this payload.
     * @see {@link #HDR_COUNT}
     * @implNote Does not modify the buffer read or write index
     * @param buffer Buffer to read from
     * @param count numerical count header value
     */
    public static void setCount(ByteBuf buffer, int count) {
        setInt(buffer, HDR_COUNT.offsetStart(), HDR_COUNT.sizeBytes(), count);
    }

    /**
     * Get the count header field value.
     * @see {@link #HDR_COUNT}
     * @implNote Does not modify the buffer read or write index
     * @param buffer
     * @return count value present in header
     */
    public static int getCount(ByteBuf buffer) {
        return getInt(buffer, HDR_COUNT.offsetStart(), HDR_COUNT.sizeBytes());
    }

    /**
     * Set the auth header value field for this header.
     * @see {@link #HDR_AUTH}
     * @implNote Does not modify the buffer read or write index
     * @param buffer Buffer to write to
     * @param auth Auth pass string to write
     */
    public static void setAuth(ByteBuf buffer, byte[] auth) {
        setBytes(buffer, HDR_AUTH.offsetStart(), auth);
    }

    /**
     * Get the auth header field. If present, indicates auth is expected to be
     * enabled.
     * @see {@link #HDR_AUTH}
     * @implNote Does not modify the buffer read or write index
     * @param buffer Buffer to read from
     * @return Auth header value
     */
    public static byte[] getAuth(ByteBuf buffer) {
        return getBytes(buffer, HDR_AUTH.offsetStart(), HDR_AUTH.sizeBytes());
    }

}