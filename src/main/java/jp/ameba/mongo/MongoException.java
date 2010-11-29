package jp.ameba.mongo;

public class MongoException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3377532781644839529L;
	
	private int code;

	public MongoException() {
	}

	public MongoException(String message) {
		super(message);
	}
	
	public MongoException(String message, int code) {
		super(message);
		this.code = code;
	}

	public MongoException(Throwable cause) {
		super(cause);
	}

	public MongoException(String message, Throwable cause) {
		super(message, cause);
	}

	public int getCode() {
		return code;
	}

}
