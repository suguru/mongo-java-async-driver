package jp.ameba.mogo.protocol;

import org.bson.BSONEncoder;
import org.bson.BSONObject;

/**
 * OP_UPDATE リクエストメッセージ
 * 
 * @author suguru
 */
public class Update extends Request {
	
	private int flags;
	
	private BSONObject selector;
	
	private BSONObject update;
	
	/**
	 * Update
	 * 
	 * @param collectionName
	 * @param upsert
	 * @param multiUpdate
	 * @param selector
	 * @param update
	 */
	public Update(
			String databaseName,
			String collectionName,
			BSONObject selector,
			BSONObject update) {
		super(OperationCode.OP_UPDATE, databaseName, collectionName);
		this.selector = selector;
		this.update = update;
		this.safeLevel = SafeLevel.SAFE;
	}
	
	/**
	 * 更新時、該当のオブジェクトが存在しなければ
	 * 新規に作成し、 insert します。
	 * 
	 * @return
	 */
	public Update upsert() {
		flags = BitWise.addBit(flags, 0);
		return this;
	}
	
	/**
	 * 複数オブジェクトの更新を設定します
	 * 
	 * @return
	 */
	public Update multiUpdate() {
		flags = BitWise.addBit(flags, 1);
		return this;
	}
	
	/**
	 * 更新における {@link SafeLevel} を設定します。
	 * 
	 * @param safeLevel
	 * @return
	 */
	public Update safeLevel(SafeLevel safeLevel) {
		setSafeLevel(safeLevel);
		return this;
	}

	@Override
	public void encode(BSONEncoder encoder) {
		// Body
		encoder.writeInt(0);
		encoder.writeCString(fullCollectionName);
		encoder.writeInt(flags);
		encoder.putObject(selector);
		encoder.putObject(update);
	}
}
