package jp.ameba.mongo;

import java.io.IOException;
import java.net.InetSocketAddress;

import jp.ameba.mongo.protocol.Delete;
import jp.ameba.mongo.protocol.Insert;
import jp.ameba.mongo.protocol.Query;
import jp.ameba.mongo.protocol.Response;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class InsertTest {

	private MongoConnection client;
	
	@Before
	public void before() throws IOException {
		MongoDriver driver = new MongoDriver();
		client = driver.createConnection(new InetSocketAddress("127.0.0.1", 27017));
		client.open();
		Assert.assertTrue(client.isOpen());
	}
	
	@After
	public void after() throws IOException {
		client.close();
		Assert.assertFalse(client.isOpen());
	}
	
	@Test
	public void testInsert() throws Exception {
		
		client.delete(new Delete("test", "insert", new BasicBSONObject("_id", 1)));
		
		// insert new document
		client.insert(
				new Insert("test", "insert",
					new BasicBSONObject("_id", 1)
						.append("name", "test-name")
						.append("integer", 100)
						.append("long", 1000L)
						.append("binary", "test binary".getBytes())
		));

		// find
		Query query = new Query("test", "insert", 0, 1, new BasicBSONObject("_id", 1), null);
		Response response = client.query(query);
		Assert.assertNotNull(response);
		Assert.assertEquals(1, response.getNumberReturned());
		Assert.assertEquals(0, response.getStartingFrom());
		Assert.assertEquals(query.getWaitingRequestId(), response.getHeader().getResponseTo());
		
		BSONObject resultObject = response.getDocuments().get(0);
		Assert.assertEquals(1, resultObject.get("_id"));
		Assert.assertEquals("test-name", resultObject.get("name"));
		Assert.assertEquals(100, resultObject.get("integer"));
		Assert.assertEquals(1000L, resultObject.get("long"));
		Assert.assertArrayEquals("test binary".getBytes(), (byte[]) resultObject.get("binary"));
		
		// Duplicate error
		MongoException me = null;
		try {
			client.insert(
					new Insert("test", "insert",
							new BasicBSONObject("_id", 1)
								.append("name", "test-name2")
					)
			);
		} catch (MongoException e) {
			me = e;
		}
		
		Assert.assertNotNull(me);
	}
	
}
