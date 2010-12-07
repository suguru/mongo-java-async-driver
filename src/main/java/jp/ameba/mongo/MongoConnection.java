package jp.ameba.mongo;

import jp.ameba.mongo.protocol.Delete;
import jp.ameba.mongo.protocol.GetMore;
import jp.ameba.mongo.protocol.Insert;
import jp.ameba.mongo.protocol.KillCursors;
import jp.ameba.mongo.protocol.Query;
import jp.ameba.mongo.protocol.Response;
import jp.ameba.mongo.protocol.Update;

/**
 * MongoDB に対する接続および通信を行うためのインターフェイス
 * 低レイヤーのプロトコル処理を実行するためのメソッドを提供します。
 * 
 * @author suguru
 */
public interface MongoConnection {

	/**
	 * この接続を識別するための ID を取得します。
	 * @return
	 */
	int getChannelId();
	
	/**
	 * 接続をオープンします。
	 * @throws MongoException
	 */
	void open() throws MongoException;
	
	/**
	 * 非同期に接続をオープンします。
	 * @return
	 * @throws MongoException
	 */
	MongoFuture openAsync() throws MongoException;
	
	/**
	 * 接続をクローズします。
	 * @throws MongoException
	 */
	void close() throws MongoException;
	
	/**
	 * 接続がオープンされているかチェックします。
	 * @return
	 */
	boolean isOpen();
	
	/**
	 * 接続されているかを確認します。
	 * @return
	 */
	boolean isConnected();
	
	/**
	 * OP_INSERT リクエストを送信します。
	 * @param insert
	 */
	void insert(Insert insert);
	
	/**
	 * OP_UPDATE リクエストを送信します。
	 * @param update
	 */
	void update(Update update);

	/**
	 * OP_DELETE リクエストを送信します。
	 * @param delete
	 */
	void delete(Delete delete);
	
	/**
	 * OP_QUERY リクエストを送信します。
	 * @param query
	 * @return
	 */
	Response query(Query query);
	
	/**
	 * OP_GET_MORE リクエストを送信します。
	 * @param getMore
	 * @return
	 */
	Response getMore(GetMore getMore);
	
	/**
	 * クエリ結果を走査するための {@link MongoCursor} を
	 * 取得します。
	 * 
	 * @param databaseName
	 * @param collectionName
	 * @return
	 */
	MongoCursor cursor(String databaseName, String collectionName);

	/**
	 * OP_KILL_CURSORS リクエストを送信します。
	 * @param killCursors
	 */
	void killCursors(KillCursors killCursors);
	
	/**
	 * 非同期に接続をクローズします。
	 * @return
	 * @throws MongoException
	 */
	MongoFuture closeAsync() throws MongoException;

}
