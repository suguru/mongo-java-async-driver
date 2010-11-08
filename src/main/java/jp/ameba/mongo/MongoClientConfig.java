package jp.ameba.mongo;

public class MongoClientConfig {

	private long connectTimeout = 10000L;
	
	private long operationTimeout = 10000L;

	public MongoClientConfig() {
	}
	
	public long getConnectTimeout() {
		return connectTimeout;
	}
	
	public long getOperationTimeout() {
		return operationTimeout;
	}
}
