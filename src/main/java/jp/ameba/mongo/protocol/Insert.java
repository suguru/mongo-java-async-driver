package jp.ameba.mongo.protocol;

import java.util.LinkedList;
import java.util.List;

import org.bson.BSONEncoder;
import org.bson.BSONObject;
import org.bson.types.ObjectId;

/**
 * OP_INSERT リクエストメッセージ
 * 
 * @author suguru
 */
public class Insert extends Request {
	
	private List<BSONObject> documents;
	
	/**
	 * Inserting a single document
	 * 
	 * @param databaseName
	 * @param collectionName
	 * @param document
	 */
	public Insert(
			String databaseName,
			String collectionName,
			BSONObject document) {
		super(OperationCode.OP_INSERT, databaseName, collectionName);
		this.documents = new LinkedList<BSONObject>();
		this.documents.add(document);
		this.consistency = Consistency.SAFE;
		if (!document.containsField("_id")) {
			document.put("_id", new ObjectId());
		}
	}
	
	/**
	 * Inserting multiple documents. (bulk insert)
	 * 
	 * @param collectionName
	 * @param upsert
	 * @param multiUpdate
	 * @param selector
	 * @param update
	 */
	public Insert(
			String databaseName,
			String collectionName,
			List<BSONObject> documents) {
		super(OperationCode.OP_INSERT, databaseName, collectionName);
		this.documents = documents;
		this.consistency = Consistency.SAFE;		
		for (BSONObject document : documents) {
			if (!document.containsField("_id")) {
				document.put("_id", new ObjectId());
			}
		}
	}
	
	/**
	 * 更新における {@link Consistency} を設定します。
	 * 
	 * @param safeLevel
	 * @return
	 */
	public Insert consistency(Consistency consistency) {
		setConsistency(consistency);
		return this;
	}

	@Override
	public void encode(BSONEncoder encoder) {
		// Body
		encoder.writeInt(0);
		encoder.writeCString(fullCollectionName);
		for (int i = 0; i < documents.size(); i++) {
			encoder.putObject(documents.get(i));
		}
	}
}
