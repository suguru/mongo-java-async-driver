package jp.ameba.mongo;

import java.net.InetSocketAddress;

import junit.framework.Assert;

import org.junit.Test;

public class OpenCloseTest {

	
	public OpenCloseTest() {
	}
	
	@Test
	public void testOpen() throws Exception {
		MongoConnection conn = new MongoDriver().createConnection(new InetSocketAddress("127.0.0.1", 27017));
		conn.open();
		Assert.assertTrue(conn.isOpen());
		conn.close();
		Assert.assertFalse(conn.isOpen());
	}
	
	@Test
	public void testOpenAsync() throws Exception {
		
		MongoConnection client = new MongoDriver().createConnection(new InetSocketAddress("127.0.0.1", 27017));
		
		// Asynchronous open
		MongoFuture future = client.openAsync();
		Assert.assertNotNull(future);
		future.await();
		Assert.assertTrue(future.isSuccess());
		
		// Asynchronous close
		future = client.closeAsync();
		Assert.assertNotNull(future);
		future.await();
		Assert.assertTrue(future.isSuccess());
		
	}
	
}
