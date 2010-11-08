package jp.ameba.mongo;

import jp.ameba.mogo.protocol.Query;
import jp.ameba.mogo.protocol.Response;
import jp.ameba.mogo.protocol.Update;
import junit.framework.Assert;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.junit.Test;

public class MongoClientTest {

	
	public MongoClientTest() {
	}
	
	@Test
	public void testOpen() throws Exception {
		
		MongoClient client = new MongoClient();
		client.setHosts("127.0.0.1");
		client.open();
		
		Assert.assertTrue(client.isOpen());
	}
	
	@Test
	public void testInsert() throws Exception {
		
	}
	
	@Test
	public void testUpdate() throws Exception {
		
		MongoClient client = new MongoClient();
		client.setHosts("127.0.0.1");
		client.open();
		
		Update update = new Update("test", "col1",
				new BasicBSONObject("_id", 1),
				new BasicBSONObject("_id", 1)
					.append("name", "test-name")
					.append("num", 100)
		).upsert();
		client.update(update);
		
		Query query = new Query("test", "col1", 0, 1, new BasicBSONObject(), null);
		Response response = client.query(query);
		Assert.assertNotNull(response);
		Assert.assertEquals(1, response.getNumberReturned());
		Assert.assertEquals(0, response.getStartingFrom());
		Assert.assertEquals(query.getWaitingRequestId(), response.getHeader().getResponseTo());
		
		BSONObject resultObject = response.getDocuments().get(0);
		Assert.assertEquals(1, resultObject.get("_id"));
		Assert.assertEquals("test-name", resultObject.get("name"));
		Assert.assertEquals(100, resultObject.get("num"));
		
	}
}
