package com.nimbus.proto.protocol;

import io.netty.buffer.ByteBuf;

public class RequestProtocol extends HeaderProtocol {

    public static final int CMD_SET = 1;
    public static final int CMD_GET = 2;
    public static final int CMD_DEL = 3;

    // COMMANDS ABOVE 100 ARE REPLICATION SPECIFIC
    public static final int REPL_CMD_ECHO = 100;

    /**
     * Start of request payload data. Currently immediately after
     * the shared header. Perhaps in the future we will reserve space
     * for request specific headers.
     */
    public static final int START_OF_DATA = HeaderProtocol.HDR_END_OFFSET;

    /**
     * Gets the command based on the numerical values
     * of the first ("major") byte of the request.
     * @param buffer
     * @see RequestProtocol.CMD_* constants
     * @return int command value
     */
    public static int getCommand(ByteBuf buffer) {
        return getMajor(buffer);
    }

    /**
     * Set the command type of this request in the major field
     * of the request header.
     * @param buffer Buffer to write to
     * @param command Command value 1 <= command <= Byte.MAX_VALUE
     */
    public static void setCommand(ByteBuf buffer, int command) {
        setMajor(buffer, command);
    }

}