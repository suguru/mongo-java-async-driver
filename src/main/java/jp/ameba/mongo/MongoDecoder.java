package jp.ameba.mongo;

import jp.ameba.mongo.protocol.MessageHeader;
import jp.ameba.mongo.protocol.OperationCode;
import jp.ameba.mongo.protocol.Response;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

public class MongoDecoder extends FrameDecoder {
	
	@Override
	protected Object decode(
			ChannelHandlerContext context,
			Channel channel,
			ChannelBuffer buffer) throws Exception {
		
		MessageHeader header = (MessageHeader) context.getAttachment();
		if (header == null) {
			header = new MessageHeader();
			context.setAttachment(header);
		}

		// ヘッダー情報が存在しない場合は、ヘッダー情報を読み込む
		if (!header.isReady()) {
			if (buffer.readableBytes() < MessageHeader.HEADER_SIZE) {
				return null;
			}
			header.read(buffer);
		}
		
		// バッファーが不足している場合は、次の読み込みまで待つ
		if (buffer.readableBytes() + 16 < header.getMessageLength()) {
			return null;
		}
		
		// 現時点で、 OP_REPLY 以外のメッセージは受け付けていない
		if (header.getOpCode() != OperationCode.OP_REPLY) {
			throw new MongoException("OP_CODE is not available in server response.");
		}
		
		// ボディ部分の読み取り
		Response response = new Response(header);
		response.readBuffer(buffer);
		context.setAttachment(null);
		return response;
	}
}