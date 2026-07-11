package com.manish.kafkalite.network;

import com.manish.kafkalite.network.protocol.Requests;
import com.manish.kafkalite.network.protocol.Responses;
import com.manish.kafkalite.storage.LogSegment;
import com.manish.kafkalite.storage.Message;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.List;

// bridge where our high-speed network layer meets our high-speed disk layer

public class KafkaLiteServerHandler extends SimpleChannelInboundHandler<Object> {

    private final LogSegment logSegment;

    public KafkaLiteServerHandler(LogSegment logSegment) {
        this.logSegment = logSegment;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {

        if (msg instanceof Requests.ProduceRequest request) {
            // 1. Write to Disk
            long offset = logSegment.append(request.payload());

            // 2. Send Response back to client
            ctx.writeAndFlush(new Responses.ProduceResponse(offset));

        } else if (msg instanceof Requests.FetchRequest request) {
            long startPos = logSegment.getPositionForOffset(request.startingOffset());

            if (startPos != -1) {
                // Calculate how many bytes remain in the file from this offset
                long bytesToTransfer = logSegment.getFileSize() - startPos;

                // 1. Send the TCP Protocol Header from the JVM
                ctx.write(new Responses.FetchHeader((int) bytesToTransfer));

                // 2. ZERO-COPY TRANSFER: Tell the OS to stream the file directly to the socket
                io.netty.channel.DefaultFileRegion region =
                        new io.netty.channel.DefaultFileRegion(logSegment.getFileChannel(), startPos, bytesToTransfer);

                ctx.writeAndFlush(region);

            } else {
                // Offset not found, send an empty response
                ctx.writeAndFlush(new Responses.FetchHeader(0));
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close(); // Close the connection if a network or disk error occurs
    }
}