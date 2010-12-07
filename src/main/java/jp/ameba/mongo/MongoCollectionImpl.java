package jp.ameba.mongo;

import java.util.Arrays;

import jp.ameba.mogo.protocol.Consistency;
import jp.ameba.mogo.protocol.Delete;
import jp.ameba.mogo.protocol.Insert;
import jp.ameba.mogo.protocol.Query;
import jp.ameba.mogo.protocol.Response;
import jp.ameba.mogo.protocol.Update;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;

/**
 * {@link MongoCollection} 実装
 * 
 * @author suguru
 */
public class MongoCollectionImpl implements MongoCollection {
	
	// デフォルトで使用する Consistency レベル
	private Consistency defaultConsistency = Consistency.SAFE;
	// 使用するクライアント
	private MongoClient client;
	// 対象のデータベース名
	private String databaseName;
	// 対象のコレクション名
	private String collectionName;
	
	/**
	 * {@link MongoCollectionImpl} を構成します。
	 * 
	 * @param client
	 * @param databaseName
	 * @param collectionName
	 */
	public MongoCollectionImpl(
			MongoClient client,
			String databaseName,
			String collectionName) {
		this.client = client;
		this.databaseName = databaseName;
		this.collectionName = collectionName;
	}
	
	/**
	 * デフォルトの一貫性レベルを設定します。
	 * @param defaultConsistency
	 */
	public void setDefaultConsistency(Consistency defaultConsistency) {
		this.defaultConsistency = defaultConsistency;
	}
	
	/**
	 * {@link MongoClient} を取得します。
	 * @return
	 */
	public MongoClient getClient() {
		return client;
	}
	
	/**
	 * データベース名を取得します。
	 * @return
	 */
	public String getDatabaseName() {
		return databaseName;
	}
	
	/**
	 * コレクション名を取得します。
	 * @return
	 */
	public String getCollectionName() {
		return collectionName;
	}

	@Override
	public void insert(BSONObject document) {
		insert(document, defaultConsistency);
	}

	@Override
	public void insert(BSONObject document, Consistency consistency) {
		client.getConnection().insert(new Insert(
				databaseName,
				collectionName,
				document
		).consistency(consistency));
	}

	@Override
	public void insert(BSONObject... documents) {
		insert(documents, defaultConsistency);
	}

	@Override
	public void insert(BSONObject[] documents, Consistency consistency) {
		client.getConnection().insert(new Insert(
				databaseName,
				collectionName,
				Arrays.asList(documents)
		).consistency(consistency));
	}
	
	@Override
	public BSONObject find(BSONObject selector) {
		return client.getConnection().query(
				new Query(
						databaseName,
						collectionName,
						0,
						1,
						selector)
				).getDocument();
	}
	
	@Override
	public BSONObject find(BSONObject selector, BSONObject fields) {
		return client.getConnection().query(
				new Query(
						databaseName,
						collectionName,
						0,
						1,
						selector,
						fields)
				).getDocument();
	}

	@Override
	public MongoCursor cursor() {
		return client.getConnection().cursor(databaseName, collectionName);
	}

	@Override
	public long count() {
		return count(new BasicBSONObject());
	}

	@Override
	public long count(BSONObject selector) {
		Response response = client.getConnection().query(
				new Query(databaseName, "$cmd", 0, 1, new BasicBSONObject()
					.append("count", collectionName)
					.append("query", selector)
				));
		BSONObject obj = response.getDocument();
		long count = ((Number) obj.get("n")).longValue();
		return count;
	}

	@Override
	public void update(BSONObject selector, BSONObject document) {
		update(selector, document, defaultConsistency);
	}

	@Override
	public void update(BSONObject selector, BSONObject document,
			Consistency consistency) {
		client.getConnection().update(
				new Update(
						databaseName,
						collectionName,
						selector,
						document
				).consistency(consistency)
		);
	}

	@Override
	public void upsert(BSONObject selector, BSONObject document) {
		upsert(selector, document, defaultConsistency);
	}

	@Override
	public void upsert(BSONObject selector, BSONObject document,
			Consistency consistency) {
		client.getConnection().update(
				new Update(
						databaseName,
						collectionName,
						selector,
						document
				).consistency(consistency).upsert()
		);
	}

	@Override
	public void remove(BSONObject selector) {
		remove(selector, defaultConsistency);
	}

	@Override
	public void remove(BSONObject selector, Consistency consistency) {
		client.getConnection().delete(
				new Delete(
						databaseName,
						collectionName,
						selector
				).consistency(consistency)
		);
	}
}
