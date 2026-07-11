package com.manish.kafkalite.network;

import com.manish.kafkalite.coordination.ConsumerCoordinator;
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
    private final ConsumerCoordinator coordinator;

    public NettyBrokerServer(int port, String logFilePath, String offsetLogPath) throws Exception {
        this.port = port;
        this.logSegment = new LogSegment(logFilePath);

        // Instantiate the coordinator pointing to our hidden offset log
        this.coordinator = new ConsumerCoordinator(offsetLogPath);
    }

    public void start() throws InterruptedException {
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new KafkaLiteDecoder());
                            ch.pipeline().addLast(new KafkaLiteEncoder());
                            // Pass BOTH the log and the coordinator to the handler
                            ch.pipeline().addLast(new KafkaLiteServerHandler(logSegment, coordinator));
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            ChannelFuture f = b.bind(port).sync();
            System.out.println("🚀 Kafka-Lite Netty Broker listening on TCP port " + port);

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
        // We now pass two files: one for data, one for consumer offsets
        NettyBrokerServer server = new NettyBrokerServer(
                9092,
                "data/netty-partition.log",
                "data/__consumer_offsets.log"
        );
        server.start();
    }
}