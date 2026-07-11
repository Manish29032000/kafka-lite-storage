package com.manish.kafkalite.network;

import com.manish.kafkalite.coordination.ConsumerCoordinator;
import com.manish.kafkalite.network.protocol.Requests;
import com.manish.kafkalite.network.protocol.Responses;
import com.manish.kafkalite.storage.LogSegment;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class KafkaLiteServerHandler extends SimpleChannelInboundHandler<Object> {

    private final LogSegment logSegment;
    private final ConsumerCoordinator coordinator;

    // Inject the coordinator into the handler
    public KafkaLiteServerHandler(LogSegment logSegment, ConsumerCoordinator coordinator) {
        this.logSegment = logSegment;
        this.coordinator = coordinator;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {

        if (msg instanceof Requests.ProduceRequest request) {
            long offset = logSegment.append(request.payload());
            ctx.writeAndFlush(new Responses.ProduceResponse(offset));

        } else if (msg instanceof Requests.FetchRequest request) {
            long startPos = logSegment.getPositionForOffset(request.startingOffset());

            if (startPos != -1) {
                long bytesToTransfer = logSegment.getFileSize() - startPos;
                ctx.write(new Responses.FetchHeader((int) bytesToTransfer));

                io.netty.channel.DefaultFileRegion region =
                        new io.netty.channel.DefaultFileRegion(logSegment.getFileChannel(), startPos, bytesToTransfer);
                ctx.writeAndFlush(region);
            } else {
                ctx.writeAndFlush(new Responses.FetchHeader(0));
            }

        } else if (msg instanceof Requests.OffsetCommitRequest request) {
            // Save the offset to the internal __consumer_offsets log
            coordinator.commitOffset(request.groupId(), request.offset());
            ctx.writeAndFlush(new Responses.OffsetCommitResponse(true));

        } else if (msg instanceof Requests.OffsetFetchRequest request) {
            // Retrieve the saved offset for this group
            long savedOffset = coordinator.getOffset(request.groupId());
            ctx.writeAndFlush(new Responses.OffsetFetchResponse(savedOffset));

        } else {
            System.err.println("Received unknown message type");
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}