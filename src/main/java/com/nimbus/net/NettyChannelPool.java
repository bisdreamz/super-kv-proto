package com.nimbus.net;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.pool.*;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;

import java.net.InetSocketAddress;
import java.time.Duration;

public class NettyChannelPool {

    private final ChannelPool pool;
    private final ClientHandler clientHandler;

    public NettyChannelPool(Bootstrap bootstrap, ClientHandler clientHandler, String host, int port, int maxConnections, Duration connectTimeout) {
        this.pool = new FixedChannelPool(bootstrap.remoteAddress(new InetSocketAddress(host, port)),
                this.getHandler(),
                ChannelHealthChecker.ACTIVE,
                FixedChannelPool.AcquireTimeoutAction.FAIL,
                connectTimeout.toMillis(),
                Math.max(maxConnections, 8),
                Math.max(maxConnections / 4, 8),
                true);
        this.clientHandler = clientHandler;
    }

    private ChannelPoolHandler getHandler() {
        return new ChannelPoolHandler() {
            @Override
            public void channelReleased(Channel ch) {
                System.out.println("Channel released: " + ch.id());
            }

            @Override
            public void channelAcquired(Channel ch) {
                System.out.println("Channel acquired: " + ch.id());
            }

            @Override
            public void channelCreated(Channel ch) {
                System.out.println("Channel created: " + ch.id());

                ChannelPipeline pipeline = ch.pipeline();

                pipeline.addLast(ProtoResponseDecoder.INSTANCE);
                pipeline.addLast(clientHandler);
            }
        };
    }

    public Future<Channel> acquire() {
        return pool.acquire();
    }

    public Future<Void> release(Channel channel) {
        return pool.release(channel);
    }

    public Future<Channel> acquire(Promise<Channel> promise) {
        return pool.acquire(promise);
    }

    public void close() {
        pool.close();
    }
}