package jp.ameba.mongo.protocol;

import org.bson.BSONEncoder;

/**
 * OP_QUERY リクエストメッセージ
 * 
 * @author suguru
 */
public class GetMore extends Request {
	
	private int numberToReturn;
	
	private long cursorId;
	
	public GetMore(
			String databaseName,
			String collectionName,
			int numberToReturn,
			long cursorId) {
		super(OperationCode.OP_GETMORE, databaseName, collectionName);
		this.numberToReturn = numberToReturn;
		this.cursorId = cursorId;
		this.consistency = Consistency.NONE;
	}

	@Override
	public void encode(BSONEncoder encoder) {
		// Body
		encoder.writeInt(0);
		encoder.writeCString(fullCollectionName);
		encoder.writeInt(numberToReturn);
		encoder.writeLong(cursorId);
	}
}
