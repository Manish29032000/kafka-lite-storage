package com.manish.kafkalite.network.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class KafkaLiteEncoder extends MessageToByteEncoder<Responses.Response> {

    @Override
    protected void encode(ChannelHandlerContext ctx, Responses.Response msg, ByteBuf out) {

        if (msg instanceof Responses.ProduceResponse response) {
            out.writeShort(1); // API Key 1 (Produce Response)
            out.writeInt(8);   // Payload Length
            out.writeLong(response.assignedOffset());

        } else if (msg instanceof Responses.FetchHeader header) {
            out.writeShort(2); // API Key 2 (Fetch)
            out.writeInt(header.payloadLength());
        }
    }
}