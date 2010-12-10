package jp.ameba.mongo;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * {@link MongoConnectionImpl} への透過的なアクセスや、
 * {@link MongoCollection} を取得するための
 * ラッパークラスです。
 * 
 * @author suguru
 */
public class MongoDatabase {
	
	// MongoClient
	private MongoClient client;
	
	// データベース名
	private String databaseName;
	
	// コレクション一覧
	private ConcurrentMap<String, MongoCollection> collections;

	/**
	 * データベースインスタンスを初期化します。
	 * 
	 * @param client
	 * @param databaseName
	 */
	public MongoDatabase(MongoClient client, String databaseName) {
		this.client = client;
		this.databaseName = databaseName;
		this.collections = new ConcurrentHashMap<String, MongoCollection>();
	}
	
	/**
	 * 内包する {@link MongoClient} を取得します。
	 * @return
	 */
	public MongoClient getClient() {
		return client;
	}
	
	/**
	 * 対象としているデータベースの名称を取得します。
	 * @return
	 */
	public String getDatabaseName() {
		return databaseName;
	}
	
	/**
	 * このデータベースに所属する {@link MongoCollection} インスタンスを取得します。
	 * @param collectionName
	 * @return
	 */
	public MongoCollection getCollection(String collectionName) {
		// キャッシュされたコレクションから取得します。
		MongoCollection collection = collections.get(collectionName);
		if (collection == null) {
			// キャシュになければ、新規に作成
			collection = new MongoCollectionImpl(client, databaseName, collectionName);
			MongoCollection oldOne = collections.putIfAbsent(collectionName, collection);
			if (oldOne != null) {
				// 作成済みのキャッシュが存在する場合は、古い方で上書き
				collection = oldOne;
			}
		}
		return collection;
	}
	
}
