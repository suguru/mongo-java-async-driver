package jp.ameba.mongo.protocol;

import jp.ameba.mongo.MongoConnection;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;

/**
 * findAndModify コマンドを実行するための
 * メソッドチェーンを持つクラスです
 * @author suguru
 */
public class FindAndModify {
	
	// 使用するコネクション
	private MongoConnection connection;
	
	// データベース名
	private String databaseName;
	
	// コレクション名
	private String collectionName;

	// 検索クエリ
	private BSONObject query;
	
	// 取得フィールド
	private BSONObject fields;
	
	// 更新内容
	private BSONObject update;
	
	// ソート内容
	private BSONObject sort;
	
	// 存在しない場合の新規作成
	private boolean upsert = false;
	
	// 更新後ドキュメントの取得
	private boolean getnew = false;
	
	// 対象削除
	private boolean remove = false;
	
	public FindAndModify(MongoConnection connection, String databaseName, String collectionName) {
		this.connection = connection;
		this.databaseName = databaseName;
		this.collectionName = collectionName;
	}
	
	/**
	 * 検索クエリ条件を設定します。
	 * @param query
	 * @return
	 */
	public FindAndModify query(BSONObject query) {
		this.query = query;
		return this;
	}
	
	/**
	 * 取得するフィールド一覧を設定します。
	 * @param fields
	 * @return
	 */
	public FindAndModify fields(BSONObject fields) {
		this.fields = fields;
		return this;
	}
	
	/**
	 * 更新内容を設定します。
	 * @param update
	 * @return
	 */
	public FindAndModify update(BSONObject update) {
		this.update = update;
		return this;
	}
	
	/**
	 * ソート内容を設定します。
	 * @param sort
	 * @return
	 */
	public FindAndModify sort(BSONObject sort) {
		this.sort = sort;
		return this;
	}
	
	/**
	 * 検索対象のドキュメントが存在しない場合に、更新内容で新規作成します。
	 * @return
	 */
	public FindAndModify upsert() {
		this.upsert = true;
		return this;
	}
	
	/**
	 * 検索対象のドキュメントを削除します。
	 * @return
	 */
	public FindAndModify remove() {
		this.remove = true;
		return this;
	}
	
	/**
	 * 更新後のドキュメント内容を取得します。
	 * @return
	 */
	public FindAndModify getnew() {
		this.getnew = true;
		return this;
	}
	
	/**
	 * findAndModify を実行します。
	 * @return
	 */
	public BSONObject execute() {
		BSONObject command = new BasicBSONObject("findAndModify", collectionName)
				.append("query", query)
				.append("update", update);
		if (sort != null) {
			command.put("sort", sort);
		}
		if (fields != null) {
			command.put("fields", fields);
		}
		if (remove) {
			command.put("remove", true);
		}
		if (upsert) {
			command.put("upsert", true);
		}
		if (getnew) {
			command.put("new", true);
		}
		Response response = connection.query(
				new Query(databaseName, "$cmd", 0, 1, command)
		);
		return (BSONObject) response.getDocument().get("value");
	}
}
