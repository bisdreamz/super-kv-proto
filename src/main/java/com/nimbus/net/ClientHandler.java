package com.nimbus.net;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.concurrent.Promise;

import java.nio.channels.ClosedChannelException;
import java.util.Map;

@ChannelHandler.Sharable
public class ClientHandler extends ChannelInboundHandlerAdapter {

    private final Map<Channel, Promise<ByteBuf>> responsePromises;

    public ClientHandler(Map<Channel, Promise<ByteBuf>> responsePromises) {
        this.responsePromises = responsePromises;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf response = (ByteBuf) msg;
        Promise<ByteBuf> promise = responsePromises.get(ctx.channel());
        if (promise != null) {
            promise.setSuccess(response);
        } else {
            response.release();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        Promise<ByteBuf> promise = responsePromises.remove(ctx.channel());
        if (promise != null) {
            promise.setFailure(cause);
        }
        ctx.close(); // Close the channel to prevent reuse
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        Promise<ByteBuf> promise = responsePromises.remove(ctx.channel());
        if (promise != null) {
            promise.setFailure(new ClosedChannelException());
        }
        super.channelInactive(ctx);
    }

}
