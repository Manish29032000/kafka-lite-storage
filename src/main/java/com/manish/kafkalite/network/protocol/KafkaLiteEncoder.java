package com.manish.kafkalite.network.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class KafkaLiteEncoder extends MessageToByteEncoder<Responses.Response> {

    @Override
    protected void encode(ChannelHandlerContext ctx, Responses.Response msg, ByteBuf out) {

        if (msg instanceof Responses.ProduceResponse response) {
            out.writeShort(1);
            out.writeInt(8);
            out.writeLong(response.assignedOffset());

        } else if (msg instanceof Responses.FetchHeader header) {
            out.writeShort(2);
            out.writeInt(header.payloadLength());

        } else if (msg instanceof Responses.OffsetCommitResponse response) {
            out.writeShort(3); // API Key 3
            out.writeInt(1);   // Payload Length (a boolean is 1 byte)
            out.writeBoolean(response.success());

        } else if (msg instanceof Responses.OffsetFetchResponse response) {
            out.writeShort(4); // API Key 4
            out.writeInt(8);   // Payload Length (a long is 8 bytes)
            out.writeLong(response.offset());
        }
    }
}