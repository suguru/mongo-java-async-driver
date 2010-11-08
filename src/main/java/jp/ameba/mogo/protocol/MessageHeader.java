package jp.ameba.mogo.protocol;

import java.util.concurrent.atomic.AtomicInteger;

import org.bson.io.OutputBuffer;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 * <p>
 * {@link MessageHeader} は、 Mongo Protocol における
 * 通信メッセージのヘッダ部分を定義します。
 * </p>
 * <p>
 * see also <a href="http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol">http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol</a>
 * </p>
 * @author suguru
 */
public class MessageHeader {

	// ヘッダのバイト数
	public static final int HEADER_SIZE = 16;
	// リクエストID採番用
	private static final AtomicInteger REQUSET_ID = new AtomicInteger();

	// このヘッダが準備完了であるか（受信時に使われます）
	private boolean ready;
	// メッセージバイト数（ヘッダ含む）
	private int messageLength;
	// リクエスト識別ID
	private int requestId;
	// このメッセージのリクエストに使われた RequestID
	private int responseTo;
	// オペレーションコード
	private OperationCode opCode;
	
	/**
	 * 
	 */
	public MessageHeader() {
		this.ready = false;
	}
	
	/**
	 * Clean data and prepare for next data.
	 */
	public void clean() {
		this.ready = false;
		this.messageLength = 0;
		this.requestId = 0;
		this.responseTo = 0;
		this.opCode = null;
	}
	
	public boolean isReady() {
		return ready;
	}
	
	/**
	 * リクエストIDをアサインします。
	 * @return
	 */
	public int assignRequestId() {
		return requestId = REQUSET_ID.incrementAndGet();
	}
	
	/**
	 * バッファから読み出します。
	 * @param buffer
	 */
	public void read(ChannelBuffer buffer) {
		messageLength = buffer.readInt();
		requestId = buffer.readInt();
		responseTo = buffer.readInt();
		opCode = OperationCode.getOpCode(buffer.readInt());
		ready = true;
	}
	
	/**
	 * バッファへ書き出します。
	 * @param buffer
	 */
	public void encode(OutputBuffer buffer) {
		buffer.writeInt(messageLength);
		buffer.writeInt(requestId);
		buffer.writeInt(responseTo);
		buffer.writeInt(opCode.getOpCode());
	}
	
	public int getMessageLength() {
		return messageLength;
	}
	
	public int getRequestId() {
		return requestId;
	}
	
	public int getResponseTo() {
		return responseTo;
	}
	
	public OperationCode getOpCode() {
		return opCode;
	}
	
	public void setMessageLength(int messageLength) {
		this.messageLength = messageLength;
	}
	
	public void setRequestId(int requestId) {
		this.requestId = requestId;
	}
	
	public void setResponseTo(int responseTo) {
		this.responseTo = responseTo;
	}
	
	public void setOpCode(OperationCode opCode) {
		this.opCode = opCode;
	}
}
