package com.manish.kafkalite.network.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class KafkaLiteDecoder extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (in.readableBytes() < 6) return;

        in.markReaderIndex();

        short apiKey = in.readShort();
        int payloadLength = in.readInt();

        if (in.readableBytes() < payloadLength) {
            in.resetReaderIndex();
            return;
        }

        if (apiKey == 1) {
            byte[] payload = new byte[payloadLength];
            in.readBytes(payload);
            out.add(new Requests.ProduceRequest(payload));

        } else if (apiKey == 2) {
            long offset = in.readLong();
            out.add(new Requests.FetchRequest(offset));

        } else if (apiKey == 3) {
            // DECODE OFFSET COMMIT (String + Long)
            short groupIdLength = in.readShort();
            byte[] groupIdBytes = new byte[groupIdLength];
            in.readBytes(groupIdBytes);
            String groupId = new String(groupIdBytes, StandardCharsets.UTF_8);

            long offset = in.readLong();
            out.add(new Requests.OffsetCommitRequest(groupId, offset));

        } else if (apiKey == 4) {
            // DECODE OFFSET FETCH (String only)
            short groupIdLength = in.readShort();
            byte[] groupIdBytes = new byte[groupIdLength];
            in.readBytes(groupIdBytes);
            String groupId = new String(groupIdBytes, StandardCharsets.UTF_8);

            out.add(new Requests.OffsetFetchRequest(groupId));

        } else {
            in.skipBytes(payloadLength);
            System.err.println("Received unknown API Key: " + apiKey);
        }
    }
}