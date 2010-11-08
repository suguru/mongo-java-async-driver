package jp.ameba.mogo.protocol;

import org.bson.BSONEncoder;

/**
 * OP_KILL_CURSORS リクエストメッセージ
 * 
 * @author suguru
 */
public class KillCursors extends Request {
	
	// カーソルID
	private long[] cursorIds;
	
	/**
	 * Kill Cursors
	 * 
	 * @param collectionName
	 * @param upsert
	 * @param multiUpdate
	 * @param selector
	 * @param update
	 */
	public KillCursors(long ... cursorIds) {
		super(OperationCode.OP_KILL_CURSORS, null, null);
		this.cursorIds = cursorIds;
		this.safeLevel = SafeLevel.NONE;
	}

	@Override
	public void encode(BSONEncoder encoder) {
		// Body
		encoder.writeInt(0);
		encoder.writeInt(cursorIds.length);
		for (int i = 0; i < cursorIds.length; i++) {
			encoder.writeLong(cursorIds[i]);
		}
	}
}
