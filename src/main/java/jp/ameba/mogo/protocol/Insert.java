package jp.ameba.mogo.protocol;

import java.util.List;

import org.bson.BSONEncoder;
import org.bson.BSONObject;

/**
 * OP_INSERT リクエストメッセージ
 * 
 * @author suguru
 */
public class Insert extends Request {
	
	private List<BSONObject> documents;
	
	/**
	 * Update
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
		this.safeLevel = SafeLevel.SAFE;		
	}
	
	/**
	 * 更新における {@link SafeLevel} を設定します。
	 * 
	 * @param safeLevel
	 * @return
	 */
	public Insert safeLevel(SafeLevel safeLevel) {
		setSafeLevel(safeLevel);
		return this;
	}

	@Override
	public void encode(BSONEncoder encoder) {
		// Body
		encoder.writeInt(0);
		encoder.writeCString(fullCollectionName);
		for (int i = 0; i < documents.size(); i++) {
			BSONObject object = documents.get(i);
			encoder.encode(object);
		}
	}
}
