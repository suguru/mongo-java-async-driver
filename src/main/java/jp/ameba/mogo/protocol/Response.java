package jp.ameba.mogo.protocol;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.bson.BSONDecoder;
import org.bson.BSONObject;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;

/**
 * OP_REPLY に相当するメッセージ情報です。
 * 
 * @author suguru
 */
public class Response {
	
	// ヘッダー情報
	private MessageHeader header;
	// フラグ
	private int responseFlags;
	// カーソルID
	private long cursorId;
	// この返信が開始しているカーソル位置
	private int startingFrom;
	// 返却されているドキュメント数
	private int numberReturned;
	// 返却されたドキュメント内容
	private List<BSONObject> documents;
	
	private static final BSONDecoder staticDecoder = new BSONDecoder();
	
	private static final List<BSONObject> emptyList = Collections.unmodifiableList(new ArrayList<BSONObject>(0));
	
	public Response(MessageHeader header) {
		this.header = header;
	}
	
	public MessageHeader getHeader() {
		return header;
	}
	
	public int getResponseFlags() {
		return responseFlags;
	}
	
	public long getCursorId() {
		return cursorId;
	}
	
	public int getNumberReturned() {
		return numberReturned;
	}
	
	public int getStartingFrom() {
		return startingFrom;
	}
	
	public boolean isCursorNotFound() {
		return BitWise.hasBit(responseFlags, 0);
	}
	
	public boolean isQueryFailrue() {
		return BitWise.hasBit(responseFlags, 1);
	}
	
	public boolean isAwaitCapable() {
		return BitWise.hasBit(responseFlags, 3);
	}
	
	/**
	 * 内包するドキュメント一覧を取得します。
	 * @return
	 */
	public List<BSONObject> getDocuments() {
		return documents;
	}
	
	/**
	 * {@link ChannelBuffer} から内容を解析・取得します。
	 * @param buffer
	 * @throws IOException
	 */
	public void readBuffer(ChannelBuffer buffer) throws IOException {
		
		responseFlags = buffer.readInt();
		cursorId = buffer.readLong();
		startingFrom = buffer.readInt();
		numberReturned = buffer.readInt();
		
		if (numberReturned == 0) {
			documents = emptyList;
		} else {
			// 残りのバッファからドキュメントを読み出す
			ChannelBufferInputStream input = new ChannelBufferInputStream(buffer);
			documents = new ArrayList<BSONObject>(numberReturned);
			for (int i = 0; i < numberReturned; i++) {
				documents.add(staticDecoder.readObject(input));
			}
		}
	}
	
}
