package jp.ameba.mongo;

import java.io.IOException;

import jp.ameba.mogo.protocol.Delete;
import jp.ameba.mogo.protocol.Insert;
import jp.ameba.mogo.protocol.Query;
import jp.ameba.mogo.protocol.Response;
import jp.ameba.mogo.protocol.Update;
import junit.framework.Assert;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class UpdateTest {

	private MongoClient client;
	
	public UpdateTest() {
	}
	
	@Before
	public void before() throws IOException {
		client = new MongoClient();
		client.setHosts("127.0.0.1");
		client.open();
		Assert.assertTrue(client.isOpen());
	}
	
	@After
	public void after() throws IOException {
		client.close();
		Assert.assertFalse(client.isOpen());
	}
	
	@Test
	public void testUpdate() throws Exception {
		
		client.delete(new Delete("test", "update", new BasicBSONObject("_id", "updateId")));
		// Test null update
		client.update(new Update("test", "update",
				new BasicBSONObject("_id", "updateId"),
				new BasicBSONObject("name", "abcdefg")
		));
		Response response = client.query(new Query("test", "update", 0, 1, new BasicBSONObject("_id", "updateId"), null));
		Assert.assertEquals(response.getNumberReturned(), 0);

		client.insert(new Insert("test", "update", new BasicBSONObject("_id", "updateId").append("name", "testname")));
		response = client.query(new Query("test", "update", 0, 1, new BasicBSONObject("_id", "updateId"), null));
		Assert.assertEquals(1, response.getNumberReturned());
		
		client.update(new Update("test", "update", new BasicBSONObject("_id", "updateId"), new BasicBSONObject("_id", "updateId").append("name", "testname updated")));
		response = client.query(new Query("test", "update", 0, 1, new BasicBSONObject("_id", "updateId"), null));
		Assert.assertEquals(1, response.getNumberReturned());
		BSONObject result = response.getDocuments().get(0);
		
		Assert.assertEquals("updateId", result.get("_id"));
		Assert.assertEquals("testname updated", result.get("name"));
		
	}
	
	@Test
	public void testUpsert() throws Exception {
		
		client.delete(new Delete("test", "update", new BasicBSONObject("_id", "updateId")));
		
		Update update = new Update("test", "update",
				new BasicBSONObject("_id", "updateId"),
				new BasicBSONObject()
					.append("_id", "updateId")
					.append("name", "test-name")
					.append("num", 100)
		).upsert();
		client.update(update);
		
		Query query = new Query("test", "update", 0, 1, new BasicBSONObject("_id", "updateId"), null);
		Response response = client.query(query);
		Assert.assertNotNull(response);
		Assert.assertEquals(1, response.getNumberReturned());
		Assert.assertEquals(0, response.getStartingFrom());
		Assert.assertEquals(query.getWaitingRequestId(), response.getHeader().getResponseTo());
		
		BSONObject resultObject = response.getDocuments().get(0);
		Assert.assertEquals("updateId", resultObject.get("_id"));
		Assert.assertEquals("test-name", resultObject.get("name"));
		Assert.assertEquals(100, resultObject.get("num"));
		
	}
	
}
