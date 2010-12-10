package jp.ameba.mongo;

import jp.ameba.mongo.protocol.Consistency;

import org.bson.BSONObject;

/**
 * 特定の MongoDB に存在する Colletion に対する
 * {@link MongoConnection} を経由した処理を簡素化して
 * 提供するためのインターフェイスです。
 * {@link MongoDatabase} から実装を取得します。
 * 
 * @author suguru
 */
public interface MongoCollection {
	
	/**
	 * このコレクションの名称を取得します。
	 * @return
	 */
	String getCollectionName();
	
	/**
	 * このコレクションの所属するデータベース名を取得します。
	 * @return
	 */
	String getDatabaseName();
	
	/**
	 * デフォルトの一貫性レベルを設定します。
	 * @param consistency
	 */
	void setDefaultConsistency(Consistency consistency);

	/**
	 * ドキュメントを挿入します。
	 * @param document
	 */
	void insert(BSONObject document);

	/**
	 * {@link Consistency} を指定してドキュメントを挿入します。
	 * @param document
	 * @param consistency
	 */
	void insert(BSONObject document, Consistency consistency);
	
	/**
	 * 複数ドキュメントのバルク挿入をします。
	 * @param documents
	 */
	void insert(BSONObject ... documents);
	
	/**
	 * {@link Consistency} を指定して複数ドキュメントのバルク挿入をします。
	 * @param documents
	 * @param consistency
	 */
	void insert(BSONObject[] documents, Consistency consistency);

	/**
	 * 指定のクエリ条件に該当するオブジェクトを取得します。
	 * @param selector
	 * @return
	 */
	BSONObject find(BSONObject selector);
	
	/**
	 * 指定のクエリ条件に該当するオブジェクトを
	 * 指定したフィールドのみ取得します。
	 * 
	 * @param selector
	 * @param fields
	 * @return
	 */
	BSONObject find(BSONObject selector, BSONObject fields);
	
	/**
	 * クエリ実行のための {@link MongoCursor} を取得します。
	 * @return
	 */
	MongoCursor cursor();
	
	/**
	 * このコレクションの件数を取得します。
	 * @return
	 */
	long count();
	
	/**
	 * このコレクションの指定条件を満たす件数を取得します。
	 * @param selector
	 * @return
	 */
	long count(BSONObject selector);
	
	/**
	 * 指定のドキュメントを更新し、更新結果を取得します。
	 * @param selector
	 * @param document
	 * @return
	 */
	BSONObject findAndModify(BSONObject selector, BSONObject document);
	
	/**
	 * 指定条件のドキュメントを更新します。
	 * @param selector
	 * @param document
	 */
	void update(BSONObject selector, BSONObject document);
	
	/**
	 * {@link Consistency} を指定して、指定条件のドキュメントを更新します。
	 * @param selector
	 * @param document
	 * @param consistency
	 */
	void update(BSONObject selector, BSONObject document, Consistency consistency);
	
	/**
	 * 指定条件のドキュメントを更新します。
	 * 条件を満たすドキュメントが存在しない場合は、新たに挿入します。
	 * @param selector
	 * @param document
	 */
	void upsert(BSONObject selector, BSONObject document);
	
	/**
	 * {@link Consistency} を指定して、指定条件のドキュメントを更新します。
	 * 条件を満たすドキュメントが存在しない場合は、新たに挿入します。
	 * @param selector
	 * @param docment
	 * @param consistency
	 */
	void upsert(BSONObject selector, BSONObject docment, Consistency consistency);

	/**
	 * 指定条件を満たすドキュメントを削除します。
	 * @param selector
	 */
	void remove(BSONObject selector);
	
	/**
	 * {@link Consistency} を指定して、条件を満たすドキュメントを削除します。
	 * @param selector
	 * @param consistency
	 */
	void remove(BSONObject selector, Consistency consistency);
	
	/**
	 * インデクスを作成します。
	 * @param keys
	 * @param options
	 */
	void createIndex(BSONObject keys, BSONObject options);
	
	/**
	 * インデクスを作成します。
	 * @param keys
	 * @param options
	 * @param consistency
	 */
	void createIndex(BSONObject keys, BSONObject options, Consistency consistency);
	
	/**
	 * コレクション全体を破棄します。
	 */
	void drop();
}
