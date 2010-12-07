package jp.ameba.mongo.protocol;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import jp.ameba.mongo.MongoException;

import org.jboss.netty.channel.ChannelFuture;

/**
 * 任意のリクエスト送出後に、
 * 
 */
public class RequestFuture implements Future<Response> {
	
	private Request request;
	
	private Request getLastError;
	
	private Response response;
	
	private ChannelFuture channelFuture;
	
	private boolean done = false;
	
	public RequestFuture(Request request) {
		this.request = request;
	}
	
	public int getRequetId() {
		return request.getHeader().getRequestId();
	}
	
	public void setGetLastError(Request getLastError) {
		this.getLastError = getLastError;
	}
	
	public Request getGetLastError() {
		return getLastError;
	}
	
	public void setResponse(Response response) {
		this.response = response;
	}
	
	public void setChannelFuture(ChannelFuture channelFuture) {
		this.channelFuture = channelFuture;
	}
	
	public ChannelFuture getChannelFuture() {
		return channelFuture;
	}
	
	public void setDone(boolean done) {
		this.done = done;
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		throw new MongoException("Cancel is not available.");
	}

	@Override
	public Response get() throws InterruptedException, ExecutionException {
		if (response == null) {
			synchronized (this) {
				if (response == null) {
					this.wait();
				}
			}
		}
		return response;
	}

	@Override
	public Response get(long timeout, TimeUnit unit)
			throws InterruptedException, ExecutionException, TimeoutException {
		if (response == null) {
			synchronized (this) {
				if (response == null) {
					this.wait(unit.toMillis(timeout));
				}
			}
		}
		if (response == null) {
			throw new TimeoutException("MongoDB operation has been timed out.");
		}
		return response;
	}

	@Override
	public boolean isCancelled() {
		return false;
	}

	@Override
	public boolean isDone() {
		return done;
	}

}
