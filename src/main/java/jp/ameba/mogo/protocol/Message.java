package jp.ameba.mogo.protocol;

import org.bson.BSONEncoder;

public class Message extends Request {
	
	private String message;
	
	public Message(String message) {
		super(OperationCode.OP_MSG, "", "");
		this.message = message;
	}

	@Override
	public void encode(BSONEncoder encoder) {
		encoder.writeCString(message);
	}
}
