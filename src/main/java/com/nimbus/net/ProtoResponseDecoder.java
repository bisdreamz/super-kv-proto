package com.nimbus.net;

import com.nimbus.proto.protocol.HeaderProtocol;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

public class ProtoResponseDecoder extends ByteToMessageDecoder {

    private static final int HEADER_SIZE = HeaderProtocol.HDR_TOTAL_LEN.sizeBytes(); // Size of the length header in bytes

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (in.readableBytes() < HEADER_SIZE)
            return;

        int length = in.getInt(in.readerIndex());

        if (in.readableBytes() < length)
            return;

        ByteBuf message = in.readSlice(length);
        out.add(message.retain());
    }

}