package com.nimbus.proto.messages;

import com.nimbus.proto.protocol.ResponseProtocol;
import io.netty.buffer.ByteBuf;
public class ResponseMessage extends BinaryMessage {

    /**
     * Create a new BinaryResponse from a request with the
     * purpose of reusing the existing byte buffer, which
     * will be immediately cleared and retained.
     * Sets status to a default of OK
     * @param request {@link RequestMessage} of which to reuse
     *                   the ByteBuf from
     */
    public ResponseMessage(RequestMessage request) {
        this(request.buffer());

        buffer.clear();
        buffer.retain();

        buffer.setIndex(ResponseProtocol.START_OF_DATA, ResponseProtocol.START_OF_DATA);
        this.status(ResponseProtocol.STATUS_OK);
    }

    public ResponseMessage(ByteBuf buffer) {
        super(buffer, ResponseProtocol.START_OF_DATA);
    }

    public ResponseMessage() {
        this(alloc.buffer());
    }

    public ResponseMessage status(int status) {
        ResponseProtocol.setStatus(buffer, status);

        return this;
    }

    public int status() {
        return ResponseProtocol.getStatus(buffer);
    }

    public ResponseMessage compression(int compression) {
        ResponseProtocol.setCompression(buffer, compression);

        return this;
    }

    public int compression() {
        return ResponseProtocol.getCompression(buffer);
    }

    public ResponseMessage count(int count) {
        ResponseProtocol.setCount(buffer, count);

        return this;
    }

    public int count() {
        return ResponseProtocol.getCount(buffer);
    }

    /**
     * Convenience method for returning a quick response
     * which contains only status and count fields, which will
     * reuse and <b>overwrite</b> the provided buffer.
     * @param buf {@link ByteBuf} to reuse which will be cleared and retained
     * @param status Status code
     * @param count Related proto count of response entries if any
     * @return A {@link ResponseMessage} of the given buffer and fields
     */
    public static ResponseMessage of(ByteBuf buf, int status, int count) {
        buf.setIndex(ResponseProtocol.START_OF_DATA, ResponseProtocol.START_OF_DATA);
        buf.clear();
        buf.retain();

        ResponseProtocol.setStatus(buf, status);
        ResponseProtocol.setCount(buf, count);

        return new ResponseMessage(buf);
    }
}