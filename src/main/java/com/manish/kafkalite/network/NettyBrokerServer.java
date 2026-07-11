package com.manish.kafkalite.network;

import com.manish.kafkalite.network.protocol.KafkaLiteDecoder;
import com.manish.kafkalite.network.protocol.KafkaLiteEncoder;
import com.manish.kafkalite.storage.LogSegment;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class NettyBrokerServer {

    private final int port;
    private final LogSegment logSegment;

    public NettyBrokerServer(int port, String logFilePath) throws Exception {
        this.port = port;
        this.logSegment = new LogSegment(logFilePath);
    }

    public void start() throws InterruptedException {
        // Boss thread accepts incoming TCP connections
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        // Worker threads handle the actual I/O (reading bytes, hitting disk)
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) {
                            // THIS IS YOUR PIPELINE
                            ch.pipeline().addLast(new KafkaLiteDecoder());
                            ch.pipeline().addLast(new KafkaLiteEncoder());
                            ch.pipeline().addLast(new KafkaLiteServerHandler(logSegment));
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)          // Max queued connections
                    .childOption(ChannelOption.SO_KEEPALIVE, true); // Keep idle connections open

            // Bind and start to accept incoming connections.
            ChannelFuture f = b.bind(port).sync();
            System.out.println("🚀 Kafka-Lite Netty Broker listening on TCP port " + port);

            // Wait until the server socket is closed.
            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
            try {
                logSegment.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        NettyBrokerServer server = new NettyBrokerServer(9092, "data/netty-partition.log");
        server.start();
    }
}