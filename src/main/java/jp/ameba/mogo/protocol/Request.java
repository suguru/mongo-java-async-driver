package jp.ameba.mogo.protocol;

import org.bson.BSONEncoder;

/**
 * {@link Request}
 * 
 * @author suguru
 */
public abstract class Request {
	
	// ヘッダ
	protected MessageHeader header;
	// データベース名
	protected String databaseName;
	// コレクション名
	protected String collectionName;
	// データベース名 + "." + コレクション名
	protected String fullCollectionName;
	// 返却待ち受け用のリクエストID
	protected int waitingRequestId;
	// リクエストの Safeレベル
	protected Consistency consistency;
	// リクエストフューチャー情報
	private RequestFuture future;
	
	/**
	 * 
	 * @param opCode
	 * @param databaseName
	 * @param colletionName
	 */
	protected Request(OperationCode opCode, String databaseName, String colletionName) {
		initHeader(opCode);
		this.databaseName = databaseName;
		this.collectionName = colletionName;
		if (databaseName != null && colletionName != null) {
			this.fullCollectionName =
				new StringBuilder(databaseName.length() + colletionName.length() + 1)
					.append(databaseName)
					.append('.')
					.append(colletionName)
					.toString();
		}
	}
	
	/**
	 * ヘッダーを初期化します。
	 * @param opCode
	 */
	protected void initHeader(OperationCode opCode) {
		header = new MessageHeader();
		header.assignRequestId();
		header.setOpCode(opCode);
		header.setResponseTo(0);
		header.assignRequestId();
	}
	
	/**
	 * メッセージヘッダーを取得します。
	 * 
	 * @return
	 */
	public MessageHeader getHeader() {
		return header;
	}
	
	/**
	 * リクエストIDを取得します。
	 * @return
	 */
	public int getRequestId() {
		return header.getRequestId();
	}
	
	/**
	 * 対象のコレクション名
	 * @return
	 */
	public String getCollectionName() {
		return collectionName;
	}
	
	/**
	 * 対象のデータベース名
	 * @return
	 */
	public String getDatabaseName() {
		return databaseName;
	}
	
	/**
	 * 返却待ち受け用のリクエストIDを取得します。
	 * @return
	 */
	public int getWaitingRequestId() {
		return waitingRequestId;
	}
	
	/**
	 * 返却待ち受け用のリクエストIDを設定します。
	 * @param waitingRequestId
	 */
	public void setWaitingRequestId(int waitingRequestId) {
		this.waitingRequestId = waitingRequestId;
	}

	/**
	 * リクエスト内容をエンコード出力します。
	 * 
	 * @param request
	 * @return
	 */
	public abstract void encode(BSONEncoder encoder);

	/**
	 * この処理に対する {@link Consistency} を取得します。
	 * OP_QUERY, OP_GETMORE に関しては適用されません。
	 * 
	 * @return
	 */
	public Consistency getConsistency() {
		return consistency;
	}

	/**
	 * この処理に対する {@link Consistency} を設定します。
	 * デフォルトは SAFE が適用されています。
	 * @param safeLevel
	 */
	public void setConsistency(Consistency consistency) {
		this.consistency = consistency;
	}
	
	/**
	 * このリクエストに関連する {@link RequestFuture} を設定します。
	 * @param future
	 */
	public void setFuture(RequestFuture future) {
		this.future = future;
	}
	
	/**
	 * このリクエストに関連する {@link RequestFuture} を取得します。
	 * @return
	 */
	public RequestFuture getFuture() {
		return future;
	}
}
