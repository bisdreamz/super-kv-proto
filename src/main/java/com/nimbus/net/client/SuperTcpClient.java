package com.nimbus.net.client;

import com.nimbus.net.ClientHandler;
import com.nimbus.net.NettyChannelPool;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class SuperTcpClient {

    private final NettyChannelPool channelPool;
    private final Map<Channel, Promise<ByteBuf>> responsePromises;
    private final ClientHandler clientHandler;

    public SuperTcpClient(String host, int port, int maxConnections, int timeoutMs, Map<ChannelOption<?>, Object> options) {
        this.responsePromises = new ConcurrentHashMap<>();
        this.clientHandler = new ClientHandler(this.responsePromises);

        System.out.println("making new client lol");
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(new NioEventLoopGroup())
                .channel(NioSocketChannel.class);

        applyOptions(bootstrap, options);

        this.channelPool = new NettyChannelPool(bootstrap, clientHandler, host, port, maxConnections, Duration.ofMillis(timeoutMs));
    }

    private void applyOptions(Bootstrap bootstrap, Map<ChannelOption<?>, Object> options) {
        for (Map.Entry<ChannelOption<?>, Object> entry : options.entrySet()) {
            applyOption(bootstrap, entry.getKey(), entry.getValue());
        }
    }

    @SuppressWarnings("unchecked")
    private <T> void applyOption(Bootstrap bootstrap, ChannelOption<T> option, Object value) {
        bootstrap.option(option, (T) value);
    }

    public CompletableFuture<ByteBuf> send(ByteBuf message) {
        CompletableFuture<ByteBuf> resultFuture = new CompletableFuture<>();

        channelPool.acquire().addListener((Future<Channel> future) -> {
            if (future.isSuccess()) {
                Channel channel = future.getNow();
                System.out.println("Acquired channel: " + channel.id());

                Promise<ByteBuf> promise = channel.eventLoop().newPromise();
                responsePromises.put(channel, promise);

                System.out.println(ByteBufUtil.hexDump(message));

                channel.writeAndFlush(message.retain()).addListener((ChannelFutureListener) writeFuture -> {
                    if (!writeFuture.isSuccess()) {
                        System.err.println("Write failed: " + writeFuture.cause());
                        responsePromises.remove(channel);
                        resultFuture.completeExceptionally(writeFuture.cause());
                        channelPool.release(channel);
                    } else {
                        System.out.println("Write succeeded on channel: " + channel.id());
                    }
                    // Do not release the channel here if write succeeded
                });

                promise.addListener((Future<ByteBuf> responseFuture) -> {
                    if (responseFuture.isSuccess()) {
                        System.out.println("Received response on channel: " + channel.id());
                        resultFuture.complete(responseFuture.getNow());
                    } else {
                        System.err.println("Response future failed: " + responseFuture.cause());
                        resultFuture.completeExceptionally(responseFuture.cause());
                    }

                    responsePromises.remove(channel);
                    channelPool.release(channel);
                });
            } else {
                System.err.println("Failed to acquire channel: " + future.cause());
                resultFuture.completeExceptionally(future.cause());
            }
        });

        return resultFuture;
    }

    public void close() {
        channelPool.close();
    }

}