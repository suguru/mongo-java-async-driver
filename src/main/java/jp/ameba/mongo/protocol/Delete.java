package jp.ameba.mongo.protocol;

import org.bson.BSONEncoder;
import org.bson.BSONObject;

/**
 * OP_UPDATE リクエストメッセージ
 * 
 * @author suguru
 */
public class Delete extends Request {
	
	private int flags;
	
	private BSONObject selector;
	
	/**
	 * Update
	 * 
	 * @param collectionName
	 * @param upsert
	 * @param multiUpdate
	 * @param selector
	 * @param update
	 */
	public Delete(
			String databaseName,
			String collectionName,
			BSONObject selector) {
		super(OperationCode.OP_DELETE, databaseName, collectionName);
		this.selector = selector;
		this.consistency = Consistency.SAFE;
	}
	
	/**
	 * selector に合致するオブジェクトを最初の
	 * 1件だけ削除します。
	 * このメソッドが呼ばれない場合、該当のオブジェクトすべてが削除されます。
	 * 
	 * @return
	 */
	public Delete singleRemove() {
		flags = BitWise.addBit(flags, 0);
		return this;
	}

	/**
	 * 更新における {@link Consistency} を設定します。
	 * 
	 * @param safeLevel
	 * @return
	 */
	public Delete consistency(Consistency consistency) {
		setConsistency(consistency);
		return this;
	}

	@Override
	public void encode(BSONEncoder encoder) {
		// Body
		encoder.writeInt(0);
		encoder.writeCString(fullCollectionName);
		encoder.writeInt(flags);
		encoder.putObject(selector);
	}
}
