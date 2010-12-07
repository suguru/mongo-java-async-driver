package jp.ameba.mongo;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ConcurrentTest {
	
	private Logger log = Logger.getLogger(ConcurrentTest.class.getName());

	private MongoClient client;
	
	private MongoCollection collection;
	
	private AtomicInteger counter;
	
	@Before
	public void before() throws IOException {
		counter = new AtomicInteger();
		client = new MongoClient();
		client.setHosts("127.0.0.1:27017");
		client.awaitOpen();
		Assert.assertTrue(client.isOpen());
		collection = client.getCollection("test", "concurrent");
		collection.remove(new BasicBSONObject());
	}
	
	@After
	public void after() throws IOException, InterruptedException {
		client.close();
		Thread.sleep(100L);
		Assert.assertFalse(client.isOpen());
	}
	
	@Test
	public void testConcurrent() throws Exception {
		log.info("Starting concurrent test");
		Assert.assertEquals(0, collection.count());
		
		int concurrent = 10;
		
		InsertThread[] insertThreads = new InsertThread[concurrent];
		for (int i = 0; i < insertThreads.length; i++) {
			insertThreads[i] = new InsertThread(i);
			insertThreads[i].start();
		}
		for (int i = 0; i < insertThreads.length; i++) {
			insertThreads[i].join();
		}
		
		Assert.assertEquals(concurrent * 1000, collection.count());
		Assert.assertEquals(concurrent * 1000, counter.get());
		
		int count = 0;
		for (BSONObject doc : collection.cursor()) {
			int key = (Integer) doc.get("_id");
			Assert.assertEquals("name-" + key, doc.get("name"));
			Assert.assertEquals("value-" + key, doc.get("value"));
			count++;
		}
		
		Assert.assertEquals(concurrent * 1000, count);
		
		counter.set(0);
		
		UpdateThread[] updateThreads = new UpdateThread[concurrent];
		for (int i = 0; i < updateThreads.length; i++) {
			updateThreads[i] = new UpdateThread(i);
			updateThreads[i].start();
		}
		for (int i = 0; i < updateThreads.length; i++) {
			updateThreads[i].join();
		}
		
		Assert.assertEquals(concurrent * 1000, collection.count());
		Assert.assertEquals(concurrent * 1000, counter.get());
		
		count = 0;
		for (BSONObject doc : collection.cursor()) {
			int key = (Integer) doc.get("_id");
			Assert.assertEquals("updated-name-" + key, doc.get("name"));
			Assert.assertEquals("value-" + key, doc.get("value"));
			count++;
		}
		
		Assert.assertEquals(concurrent * 1000, count);
		
		counter.set(0);
		
		QueryThread[] queryThreads = new QueryThread[concurrent];
		for (int i = 0; i < queryThreads.length; i++) {
			queryThreads[i] = new QueryThread();
			queryThreads[i].start();
		}
		for (int i = 0; i < queryThreads.length; i++) {
			queryThreads[i].join();
		}
		
		Assert.assertEquals(concurrent * 1000 * concurrent, counter.get());
		
		
		counter.set(0);
		
		RemoveThread[] removeThreads = new RemoveThread[concurrent];
		for (int i = 0; i < removeThreads.length; i++) {
			removeThreads[i] = new RemoveThread(i);
			removeThreads[i].start();
		}
		for (int i = 0; i < removeThreads.length; i++) {
			removeThreads[i].join();
		}
		
		Assert.assertEquals(0, collection.count());
		
	}
	
	private class InsertThread extends Thread {
		private int id;
		private InsertThread(int id) {
			this.id = id;
		}
		@Override
		public void run() {
			for (int i = 0; i < 1000; i++) {
				int key = id * 1000 + i;
				BSONObject obj = new BasicBSONObject("_id", key)
				.append("name","name-" + key)
				.append("value", "value-" + key);
				collection.insert(obj);
				int inserted = 0;
				if ((inserted = counter.incrementAndGet()) % 2000 == 0) {
					log.info("Inserted " + inserted + " records");
				}
				BSONObject saved = collection.find(new BasicBSONObject("_id", key));
				Assert.assertNotNull(saved);
				Assert.assertEquals(obj, saved);
			}
		}
	}
	
	private class UpdateThread extends Thread {
		private int id;
		public UpdateThread(int id) {
			this.id = id;
		}
		@Override
		public void run() {
			for (int i = 0; i < 1000; i++) {
				int key = id * 1000 + i;
				BSONObject obj = new BasicBSONObject("$set",new BasicBSONObject("name", "updated-name-" + key));
				collection.update(new BasicBSONObject("_id", key), obj);
				int updated = 0;
				if ((updated = counter.incrementAndGet()) % 2000 == 0) {
					log.info("Updated " + updated + " records");
				}
				BSONObject saved = collection.find(new BasicBSONObject("_id", key));
				Assert.assertNotNull(saved);
				Assert.assertEquals(key, saved.get("_id"));
				Assert.assertEquals("updated-name-" + key, saved.get("name"));
				Assert.assertEquals("value-" + key, saved.get("value"));
			}
		}
	}
	
	private class RemoveThread extends Thread {
		private int id;
		public RemoveThread(int id) {
			this.id = id;
		}
		@Override
		public void run() {
			for (int i = 0; i < 1000; i++) {
				int key = id * 1000 + i;
				Assert.assertEquals(1, collection.count(new BasicBSONObject("_id", key)));
				collection.remove(new BasicBSONObject("_id", key));
				Assert.assertEquals(0, collection.count(new BasicBSONObject("_id", key)));
				Assert.assertNull(collection.find(new BasicBSONObject("_id", key)));
				int removed = 0;
				if ((removed = counter.incrementAndGet()) % 2000 == 0) {
					log.info("Removed " + removed + " records");
				}
			}
		}
	}
	
	private class QueryThread extends Thread {
		@Override
		public void run() {
			for (BSONObject doc : collection.cursor()) {
				int key = (Integer) doc.get("_id");
				Assert.assertEquals("updated-name-" + key, doc.get("name"));
				Assert.assertEquals("value-" + key, doc.get("value"));
				int selected = 0;
				if ((selected = counter.incrementAndGet()) % 10000 == 0) {
					log.info("Selecte " + selected + " records");
				}
			}
		}
	}
}
