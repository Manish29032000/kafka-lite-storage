package com.manish.kafkalite.network.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.util.List;

public class KafkaLiteDecoder extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        // 1. We need at least 6 bytes to read the header (2 byte API Key + 4 byte Length)
        if (in.readableBytes() < 6) {
            return;
        }

        // 2. Mark our current position in the buffer. If the full payload hasn't arrived yet,
        // we will reset back to this mark and wait for more network packets.
        in.markReaderIndex();

        short apiKey = in.readShort();
        int payloadLength = in.readInt();

        // 3. Do we have the full payload yet?
        if (in.readableBytes() < payloadLength) {
            in.resetReaderIndex(); // Not enough data, rewind and wait
            return;
        }

        // 4. We have the full message! Decode based on the API Key.
        if (apiKey == 1) {
            // Handle Produce Request
            byte[] payload = new byte[payloadLength];
            in.readBytes(payload);
            out.add(new Requests.ProduceRequest(payload));

        } else if (apiKey == 2) {
            // Handle Fetch Request (Payload should be exactly an 8-byte long offset)
            long offset = in.readLong();
            out.add(new Requests.FetchRequest(offset));

        } else {
            // Unknown API Key: Skip the bytes to prevent buffer blocking
            in.skipBytes(payloadLength);
            System.err.println("Received unknown API Key: " + apiKey);
        }
    }
}