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
	
	private MongoConnectionImpl client;
	
	private String databaseName;
	
	private ConcurrentMap<String, MongoCollection> collections;

	/**
	 * データベースインスタンスを初期化します。
	 * 
	 * @param client
	 * @param databaseName
	 */
	public MongoDatabase(MongoConnectionImpl client, String databaseName) {
		this.client = client;
		this.databaseName = databaseName;
		this.collections = new ConcurrentHashMap<String, MongoCollection>();
	}
	
	/**
	 * 内包する {@link MongoConnectionImpl} を取得します。
	 * @return
	 */
	public MongoConnectionImpl getClient() {
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
			MongoCollection oldOne = collections.putIfAbsent(collectionName, null);
			if (oldOne != null) {
				// 作成済みのキャッシュが存在する場合は、古い方で上書き
				collection = oldOne;
			}
		}
		return collection;
	}
	
}
