package com.nimbus.proto.protocol;

import io.netty.buffer.ByteBuf;

public class ResponseProtocol extends HeaderProtocol {

    public static final int STATUS_OK = 0x01;
    public static final int STATUS_KEY_UNKNOWN = 0x02;
    public static final int STATUS_INVALID_REQ = 0x03;

    /**
     * Static offset into buffer where response payload data begins,
     * after the fixed sized header housing common fields
     */
    public static final int START_OF_DATA = HeaderProtocol.HDR_END_OFFSET;

    /**
     * Sets the response status into the major field of the response header
     * @param buffer Buffer to write to
     * @param status Status numerical value in range of bytes supported by {@link HeaderProtocol#HDR_MAJOR}
     * @return This for builder pattern
     */
    public static ByteBuf setStatus(ByteBuf buffer, int status) {
        setMajor(buffer, status);

        return buffer;
    }

    /**
     * Get the existing status value from this response's major
     * header field.
     * @param buffer Buffer to read from
     * @return numerical status value
     */
    public static int getStatus(ByteBuf buffer) {
        return getMajor(buffer);
    }

}