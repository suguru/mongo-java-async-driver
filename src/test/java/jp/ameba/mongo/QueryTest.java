package jp.ameba.mongo;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

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

public class QueryTest {

	private MongoConnection client;
	
	@Before
	public void before() throws IOException {
		client = new MongoDriver().createConnection(new InetSocketAddress("127.0.0.1", 27017));
		client.open();
		Assert.assertTrue(client.isOpen());
		prepareData();
	}
	
	@After
	public void after() throws IOException {
		client.close();
		Assert.assertFalse(client.isOpen());
	}
	
	@Test
	public void testQuery() throws Exception {
		
		// normal query
		Response response = client.query(new Query("test", "testQuery", 0, 100, new BasicBSONObject()));
		Assert.assertEquals(100, response.getNumberReturned());
		Assert.assertEquals(100, response.getDocuments().size());
		for (BSONObject doc : response.getDocuments()) {
			int id = ((Number) doc.get("_id")).intValue();
			Assert.assertNotNull(doc);
			Assert.assertEquals(id, doc.get("_id"));
			Assert.assertEquals("name-" + id, doc.get("name"));
			Assert.assertEquals("value-" + (id*100), doc.get("value"));
		}
		
		List<BSONObject> first10 = response.getDocuments().subList(0, 10);
		List<BSONObject> next10 = response.getDocuments().subList(10, 20);
		
		// limited query
		response = client.query(new Query("test", "testQuery", 0, 10, new BasicBSONObject()));
		Assert.assertEquals(10, response.getNumberReturned());
		Assert.assertEquals(10, response.getDocuments().size());
		Assert.assertEquals(first10, response.getDocuments());
		
		// skipped query
		response = client.query(new Query("test", "testQuery", 10, 10, new BasicBSONObject()));
		Assert.assertEquals(10, response.getNumberReturned());
		Assert.assertEquals(10, response.getDocuments().size());
		Assert.assertEquals(next10, response.getDocuments());
		
		// over skipped query
		response = client.query(new Query("test", "testQuery", 10, 100, new BasicBSONObject()));
		Assert.assertEquals(90, response.getNumberReturned());
		Assert.assertEquals(90, response.getDocuments().size());
		
		// equals query
		response = client.query(new Query("test", "testQuery", 0, 1, new BasicBSONObject("_id", 50)));
		Assert.assertEquals(1, response.getNumberReturned());
		Assert.assertEquals(1, response.getDocuments().size());
		BSONObject doc = response.getDocuments().get(0);
		Assert.assertEquals(50, doc.get("_id"));
		
		// greater than query
		response = client.query(new Query("test", "testQuery", 0, 100,
				new BasicBSONObject("_id", new BasicBSONObject("$gt", 50))));
		Assert.assertEquals(49, response.getNumberReturned());
		Assert.assertEquals(49, response.getDocuments().size());
		response = client.query(new Query("test", "testQuery", 0, 100,
				new BasicBSONObject("_id", new BasicBSONObject("$gte", 50))));
		Assert.assertEquals(50, response.getNumberReturned());
		Assert.assertEquals(50, response.getDocuments().size());
	
		// lesser than query
		response = client.query(new Query("test", "testQuery", 0, 100,
				new BasicBSONObject("_id", new BasicBSONObject("$lt", 50))));
		Assert.assertEquals(50, response.getNumberReturned());
		Assert.assertEquals(50, response.getDocuments().size());
		response = client.query(new Query("test", "testQuery", 0, 100,
				new BasicBSONObject("_id", new BasicBSONObject("$lte", 50))));
		Assert.assertEquals(51, response.getNumberReturned());
		Assert.assertEquals(51, response.getDocuments().size());
		
		// combined query
		response = client.query(new Query("test", "testQuery", 0, 100,
				new BasicBSONObject("_id", new BasicBSONObject("$gt", 20).append("$lt", 80))));
		Assert.assertEquals(59, response.getNumberReturned());
		Assert.assertEquals(59, response.getDocuments().size());
		
	}
	
	@Test
	public void testSort() throws Exception {
		
		// order integer
		Response response = client.query(new Query("test", "testQuery", 0, 100,
				new BasicBSONObject("$query",new BasicBSONObject())
					.append("$orderby", new BasicBSONObject("_id", 1))));
		Assert.assertEquals(100, response.getNumberReturned());
		for (int i = 0; i < 100; i++) {
			BSONObject doc = response.getDocuments().get(i);
			Assert.assertEquals(i, doc.get("_id"));
		}
		
		// order integer (reversed)
		response = client.query(new Query("test", "testQuery", 0, 100,
				new BasicBSONObject("$query", new BasicBSONObject())
				.append("$orderby", new BasicBSONObject("_id", -1))));
		Assert.assertEquals(100, response.getNumberReturned());
		for (int i = 0; i < 100; i++) {
			BSONObject doc = response.getDocuments().get(i);
			Assert.assertEquals(99-i, doc.get("_id"));
		}
		
	}
	
	@Test
	public void testReturnFields() throws Exception {
		
		Response response = client.query(new Query(
				"test", "testQuery", 0, 100, new BasicBSONObject(), new BasicBSONObject("name", 1)
		));
		
		Assert.assertEquals(100, response.getNumberReturned());
		Assert.assertEquals(100, response.getDocuments().size());
		
		for (int i = 0; i < 100; i++) {
			BSONObject doc = response.getDocuments().get(i);
			Assert.assertTrue(doc.containsField("_id"));
			Assert.assertTrue(doc.containsField("name"));
			Assert.assertFalse(doc.containsField("value"));
		}
	}
	
	@Test
	public void testCursor() throws Exception {
		MongoCursor cursor = client.cursor("test", "testQuery");
		int count = 0;
		while (cursor.hasNext()) {
			BSONObject doc = cursor.next();
			Assert.assertNotNull(doc);
			count++;
		}
		Assert.assertEquals(100, count);
		
		// batch size
		cursor = client.cursor("test", "testQuery").batchSize(10);
		count = 0;
		while (cursor.hasNext()) {
			BSONObject doc = cursor.next();
			Assert.assertNotNull(doc);
			count++;
		}
		Assert.assertEquals(100, count);
	}
	
	/**
	 * データ準備
	 */
	private void prepareData() {
		
		Response response = client.query(new Query("test", "testQuery", 0, 100, new BasicBSONObject(), null));
		client.delete(new Delete("test", "testQuery", new BasicBSONObject()));
		response = client.query(new Query("test", "$cmd", 0, 1, new BasicBSONObject("count", "testQuery"), null));
		
		Assert.assertEquals(1, response.getNumberReturned());
		BSONObject result = response.getDocuments().get(0);
		Assert.assertEquals(0.0, result.get("n"));
		
		// prepare data
		for (int i = 0; i < 100; i++) {
			BSONObject doc = createObject(i);
			client.insert(new Insert("test", "testQuery", doc));
		}
	}
	
	private BSONObject createObject(int number) {
		return new BasicBSONObject("_id", number)
			.append("name", "name-" + number)
			.append("value", "value-" + (number * 100));
	}
}
