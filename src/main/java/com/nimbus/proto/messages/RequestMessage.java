package com.nimbus.proto.messages;

import com.nimbus.proto.protocol.RequestProtocol;
import io.netty.buffer.ByteBuf;

public class RequestMessage extends BinaryMessage {

    public RequestMessage(ByteBuf buffer) {
        super(buffer, RequestProtocol.START_OF_DATA);

        if (buffer.writerIndex() < this.startOfData)
            this.resetWriteIndex();
        this.resetReadIndex();
    }

    public int command() {
        return RequestProtocol.getCommand(buffer);
    }

    public void command(int command) {
        RequestProtocol.setCommand(buffer, command);
    }

    public int compression() {
        return RequestProtocol.getCompression(buffer);
    }

    public void compression(int compression) {
        RequestProtocol.setCompression(buffer, compression);
    }

    public int count() {
        return RequestProtocol.getCount(buffer);
    }

    public void count(int count) {
        RequestProtocol.setCount(buffer, count);
    }

}