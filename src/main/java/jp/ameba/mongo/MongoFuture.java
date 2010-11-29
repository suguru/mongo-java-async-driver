package jp.ameba.mongo;

import java.util.concurrent.TimeUnit;

/**
 * MongoDB の接続・切断時に
 * 
 * @author suguru
 */
public class MongoFuture {
	
	private boolean success = false;
	
	public MongoFuture() {
	}
	
	public void setSuccess(boolean success) {
		this.success = success;
	}
	
	public boolean isSuccess() {
		return success;
	}

	public void await() throws InterruptedException {
		if (success) {
			return;
		}
		synchronized (this) {
			wait();
		}
	}
	
	public void await(long timeout, TimeUnit unit) throws InterruptedException {
		synchronized (this) {
			if (success) {
				return;
			}
			wait(unit.toMillis(timeout));
		}
	}

}
