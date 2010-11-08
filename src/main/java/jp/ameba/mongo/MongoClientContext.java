package jp.ameba.mongo;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import jp.ameba.mogo.protocol.Request;
import jp.ameba.mogo.protocol.RequestFuture;

/**
 * 
 * @author suguru
 */
public class MongoClientContext {

	// 設定
	private MongoClientConfig config;
	
	// 返信がくるべき送信リクエスト一覧
	private ConcurrentMap<Integer, RequestFuture> requestMap;

	/**
	 * 
	 */
	public MongoClientContext() {
		this.config = new MongoClientConfig();
		this.requestMap = new ConcurrentHashMap<Integer, RequestFuture>();
	}
	
	public void addRequest(RequestFuture future) {
		Request lastError = future.getGetLastError();
		if (lastError == null) {
			requestMap.put(future.getRequetId(), future);
		} else {
			requestMap.put(lastError.getHeader().getRequestId(), future);
		}
	}
	
	public RequestFuture removeFuture(int requestId) {
		return requestMap.remove(requestId);
	}
	
	public RequestFuture getFuture(int requestId) {
		return requestMap.get(requestId);
	}
	
	public MongoClientConfig getConfig() {
		return config;
	}
}
