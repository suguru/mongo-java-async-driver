package jp.ameba.mongo;

import junit.framework.Assert;

import org.junit.Test;

public class OpenCloseTest {

	
	public OpenCloseTest() {
	}
	
	@Test
	public void testOpen() throws Exception {
		MongoClient client = new MongoClient();
		client.setHosts("127.0.0.1");
		client.open();
		Assert.assertTrue(client.isOpen());
		client.close();
		Assert.assertFalse(client.isOpen());
	}
	
	@Test
	public void testOpenAsync() throws Exception {
		
		MongoClient client = new MongoClient();
		client.setHosts("127.0.0.1");
		
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
