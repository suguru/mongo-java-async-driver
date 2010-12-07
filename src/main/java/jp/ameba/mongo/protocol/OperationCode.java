package jp.ameba.mongo.protocol;

/**
 * Mongo Wire Protocol における
 * オペレーションコードです。
 * 
 * @author suguru
 *
 */
public enum OperationCode {

	OP_REPLY(1, false),
	OP_MSG(1000, true),
	OP_UPDATE(2001, false),
	OP_INSERT(2002, false),
	OP_GET_BY_OID(2003, true),
	OP_QUERY(2004, true),
	OP_GETMORE(2005, true),
	OP_DELETE(2006, false),
	OP_KILL_CURSORS(2007, false),
	;
	
	private int opCode;
	
	private boolean hasReply = false;
	
	private OperationCode(int opCode, boolean hasReply) {
		this.opCode = opCode;
		this.hasReply = hasReply;
	}
	
	public int getOpCode() {
		return opCode;
	}
	
	public boolean hasReply() {
		return hasReply;
	}
	
	public static OperationCode getOpCode(int opCode) {
		switch (opCode) {
		case 1:
			return OP_REPLY;
		case 1000:
			return OP_MSG;
		case 2001:
			return OP_UPDATE;
		case 2002:
			return OP_INSERT;
		case 2003:
			return OP_GET_BY_OID;
		case 2004:
			return OP_QUERY;
		case 2005:
			return OP_GETMORE;
		case 2006:
			return OP_DELETE;
		case 2007:
			return OP_KILL_CURSORS;
		}
		return null;
	}
}
