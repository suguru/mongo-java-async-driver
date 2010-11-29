package jp.ameba.mongo;

import jp.ameba.mogo.protocol.Consistency;

import org.bson.BSONObject;

/**
 * 特定の MongoDB に存在する Colletion に対する
 * {@link MongoClient} を経由した処理を簡素化して
 * 提供するためのインターフェイスです。
 * {@link MongoDatabase} から実装を取得します。
 * 
 * @author suguru
 */
public interface MongoCollection {

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
	 * @param criteria
	 * @return
	 */
	long count(BSONObject criteria);
	
	/**
	 * 指定条件のドキュメントを更新します。
	 * @param criteria
	 * @param document
	 */
	void update(BSONObject criteria, BSONObject document);
	
	/**
	 * {@link Consistency} を指定して、指定条件のドキュメントを更新します。
	 * @param criteria
	 * @param document
	 * @param consistency
	 */
	void update(BSONObject criteria, BSONObject document, Consistency consistency);
	
	/**
	 * 指定条件のドキュメントを更新します。
	 * 条件を満たすドキュメントが存在しない場合は、新たに挿入します。
	 * @param criteria
	 * @param document
	 */
	void upsert(BSONObject criteria, BSONObject document);
	
	/**
	 * {@link Consistency} を指定して、指定条件のドキュメントを更新します。
	 * 条件を満たすドキュメントが存在しない場合は、新たに挿入します。
	 * @param criteria
	 * @param docment
	 * @param consistency
	 */
	void upsert(BSONObject criteria, BSONObject docment, Consistency consistency);

	/**
	 * 指定条件を満たすドキュメントを削除します。
	 * @param criteria
	 */
	void remove(BSONObject criteria);
	
	/**
	 * {@link Consistency} を指定して、条件を満たすドキュメントを削除します。
	 * @param criteria
	 * @param consistency
	 */
	void remove(BSONObject criteria, Consistency consistency);
}
