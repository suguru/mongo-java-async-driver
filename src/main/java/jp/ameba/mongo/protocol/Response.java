package jp.ameba.mongo.protocol;

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
	
	/**
	 * メッセージヘッダを設定し、レスポンスオブジェクトを作成します。
	 * @param header
	 */
	public Response(MessageHeader header) {
		this.header = header;
	}
	
	/**
	 * メッセージヘッダを取得します。
	 * @return
	 */
	public MessageHeader getHeader() {
		return header;
	}
	
	/**
	 * レスポンスフラグを取得します。
	 * @return
	 */
	public int getResponseFlags() {
		return responseFlags;
	}
	
	/**
	 * このレスポンスの持つカーソルIDを取得します。
	 * @return
	 */
	public long getCursorId() {
		return cursorId;
	}
	
	/**
	 * 返却されたドキュメント数を取得します。
	 * @return
	 */
	public int getNumberReturned() {
		return numberReturned;
	}
	
	/**
	 * 結果の開始位置を取得します。
	 * @return
	 */
	public int getStartingFrom() {
		return startingFrom;
	}
	
	/**
	 * CURSOR NOT FOUND が発生しているか確認します。
	 * @return
	 */
	public boolean isCursorNotFound() {
		return BitWise.hasBit(responseFlags, 0);
	}
	
	/**
	 * このレスポンスが正常に終了した結果であるかを確認します。
	 * From MongoDB Java driver (CommandResult)
	 * @return
	 */
	public boolean isOk() {
		if (BitWise.hasBit(responseFlags, 1)) {
			return false;
		}
    	if (documents.size() == 0) {
    		return true;
    	}
    	BSONObject doc = documents.get(0);
        Object o = doc.get("ok");
        if (o == null) {
            return true;
        }
        if (o instanceof Boolean) {
            return (Boolean) o;
        }
        if (o instanceof Number) {
            return ((Number) o).intValue() == 1;
        }
        throw new IllegalArgumentException("Illegal class '" + o.getClass().getName() + "'");
	}
	
	/**
	 * このレスポンスが AWAIT CAPABLE であるか確認します。
	 * @return
	 */
	public boolean isAwaitCapable() {
		return BitWise.hasBit(responseFlags, 3);
	}
	
    /**
     * 失敗している場合のエラーメッセージを取得します。
     * From MongoDB Java driver (CommandResult)
     * @return
     */
    public String getErrorMessage(){
    	if (documents.size() == 0) {
    		return null;
    	}
    	BSONObject doc = documents.get(0);
        Object errorMessage = doc.get("errmsg");
        return errorMessage == null ? null : errorMessage.toString();
    }
    
	/**
	 * 内包するドキュメント一覧を取得します。
	 * @return
	 */
	public List<BSONObject> getDocuments() {
		return documents;
	}
	
	/**
	 * 内包するドキュメントの最初の1つを取得します。
	 * @return
	 */
	public BSONObject getDocument() {
		if (documents.size() == 0) {
			return null;
		}
		return documents.get(0);
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
