package jp.ameba.mongo;

import java.util.Iterator;
import java.util.List;

import jp.ameba.mogo.protocol.Delete;
import jp.ameba.mogo.protocol.GetMore;
import jp.ameba.mogo.protocol.KillCursors;
import jp.ameba.mogo.protocol.Query;
import jp.ameba.mogo.protocol.Response;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;

/**
 * カーソル処理を行ないます。
 * 
 * @author suguru
 */
public class MongoCursor implements Iterable<BSONObject>, Iterator<BSONObject> {
	
	// MongoClient
	private MongoClient client;
	
	// 検索に利用したクエリ
	private Query query;
	
	// カーソルから取得した最新の結果
	private Response lastResult;
	
	// 現在選択中のオブジェクト
	private BSONObject currentObject;
	
	// 現在の結果におけるインデクス
	private int indexInResult;
	
	// カーソル最終位置への到達フラグ
	private boolean finished;
	
	// カーソルのクローズフラグ
	private boolean closed;

	MongoCursor(
			MongoClient client,
			Query query,
			Response lastResult) {
		this.client = client;
		this.query = query;
		this.lastResult = lastResult;
		this.indexInResult = 0;
		this.finished = lastResult.getNumberReturned() == 0;
		this.closed = false;
	}
	
	@Override
	public boolean hasNext() {
		if (finished || closed) {
			return false;
		}
		List<BSONObject> documents = lastResult.getDocuments();
		if (documents == null || indexInResult > documents.size()) {
			lastResult = client.getMore(new GetMore(
					query.getDatabaseName(),
					query.getCollectionName(),
					query.getNumberToReturn(),
					lastResult.getCursorId()
			));
			indexInResult = 0;
			if (lastResult.getNumberReturned() == 0) {
				finished = true;
				close();
				return false;
			}
		}
		return true;
	}
	
	@Override
	public BSONObject next() {
		if (!hasNext()) {
			return null;
		}
		List<BSONObject> documents = lastResult.getDocuments();
		return currentObject = documents.get(indexInResult++);
	}
	
	@Override
	public void remove() {
		if (currentObject != null) {
			Delete delete = new Delete(
					query.getDatabaseName(),
					query.getCollectionName(),
					new BasicBSONObject("_id", currentObject.get("_id"))
			);
			client.delete(delete);
		}
	}
	
	@Override
	public Iterator<BSONObject> iterator() {
		return this;
	}
	
	@Override
	protected void finalize() throws Throwable {
		if (!closed) {
			close();
		}
	}

	public void close() {
		if (!closed) {
			client.killCursors(new KillCursors(lastResult.getCursorId()));
			closed = true;
		}
	}
}
