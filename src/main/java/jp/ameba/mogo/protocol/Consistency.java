package jp.ameba.mogo.protocol;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;

public enum Consistency {

	NONE(null),
	SAFE(new BasicBSONObject("getlasterror", 1)),
	FSYNC(new BasicBSONObject("getlasterror", 1).append("fsync", true)),
	REPLICATION(new BasicBSONObject("getlasterror", 1).append("w", 2))
	
	;
	
	private BSONObject getLastError;

	private Consistency(BSONObject object) {
		this.getLastError = object;
	}
	
	public BSONObject getLastErrorQuery() {
		return getLastError;
	}
}
