package jp.ameba.mongo;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.netty.channel.Channel;


/**
 * {@link MongoConnection} を利用し、{@link MongoCollection} を取得し
 * データベース間のやり取りを簡素化するためのクライアント実装です。
 * Sharding 構成による複数サーバーへの接続切り替えに対応しています。
 * 
 * @author suguru
 */
public class MongoClient {
	
	// Channel Handler
	private MongoChannelHandler channelHandler;
	
	// 接続のためのドライバーインスタンス
	private MongoDriver driver;
	
	// 接続 Round Robin
	private AtomicInteger roundRobinKey;
	
	// MongoConnection の Channel ID 対応表
	private ConcurrentMap<Integer, MongoConnection> connectionMap;
	
	// MongoDatabase 対応表
	private ConcurrentMap<String, MongoDatabase> databaseMap;

	public MongoClient() {
		this.channelHandler = new MongoChannelHandler();
		this.driver = new MongoDriver(channelHandler);
		this.roundRobinKey = new AtomicInteger();
		this.connectionMap = new ConcurrentHashMap<Integer, MongoConnection>();
		this.databaseMap = new ConcurrentHashMap<String, MongoDatabase>();
	}
	
	/**
	 * 有効な接続が存在するか確認します。
	 * @return
	 */
	public boolean isOpen() {
		return !channelHandler.getLiveChannelList().isEmpty();
	}
	
	/**
	 * 接続をクローズします。
	 */
	public void close() {
		for (MongoConnection connection : connectionMap.values()) {
			connection.closeAsync();
		}
	}
	
	/**
	 * ホスト文字列を設定し、サーバー接続を開始します。
	 * ホスト文字列は、半角カンマで区切ることによって複数してい可能です。
	 * ポート番号の指定がない場合は、デフォルトポートの 27017 が使用されます。
	 * ex) mongodb01:27017, mongodb02:27018, mongodb03
	 * @param hosts
	 */
	public void setHosts(String hosts) {
		if (hosts == null) {
			return;
		}
		String[] hostArray = hosts.split(",");
		for (String host : hostArray) {
			host = host.trim();
			int port = 27017;
			int i = host.indexOf(':');
			if (i >= 0) {
				port = Integer.parseInt(host.substring(i+1));
				host = host.substring(0, i).trim();
			}
			addServer(host, port);
		}
	}
	
	/**
	 * クライアント設定オブジェクトを取得します。
	 * @return
	 */
	public MongoConfiguration getConfiguration() {
		return driver.getConfiguration();
	}
	
	/**
	 * 使用するサーバー接続を追加します。
	 * @param host
	 * @param port
	 */
	public void addServer(String host, int port) {
		addServer(new InetSocketAddress(host, port));
	}
	
	/**
	 * 使用するサーバー接続を追加します。
	 * @param socketAddress
	 */
	public void addServer(SocketAddress socketAddress) {
		MongoConnection connection = driver.createConnection(socketAddress);
		connection.openAsync();
		connectionMap.put(
				connection.getChannelId(),
				connection
		);
	}
	
	/**
	 * 接続を取得します。
	 * @return
	 */
	public MongoConnection getConnection() {
		List<Channel> connectionList = channelHandler.getLiveChannelList();
		if (connectionList.size() == 0) {
			awaitOpen();
		}
		int index = roundRobinKey.getAndIncrement() % connectionList.size();
		Channel channel = connectionList.get(index);
		return connectionMap.get(channel.getId());
	}
	
	/**
	 * 
	 * @param databaseName
	 * @return
	 */
	public MongoDatabase getDatabase(String databaseName) {
		MongoDatabase database = databaseMap.get(databaseName);
		if (database == null) {
			database = new MongoDatabase(this, databaseName);
			MongoDatabase oldDatabase = databaseMap.putIfAbsent(databaseName, database);
			if (oldDatabase != null) {
				return oldDatabase;
			}
		}
		return database;
	}
	
	/**
	 * コレクション名を指定して、
	 * {@link MongoCollection} インスタンスを取得します。
	 * 
	 * @param collectionName
	 * @return
	 */
	public MongoCollection getCollection(String databaseName, String collectionName) {
		MongoDatabase database = getDatabase(databaseName);
		if (database != null) {
			return database.getCollection(collectionName);
		}
		return null;
	}

	/**
	 * 1つでも利用可能な接続があるまで待ちます。
	 * 接続タイムアウト時間を過ぎても接続が確立されない場合は、
	 * {@link MongoException} をスローします。
	 * @param timeout
	 */
	public void awaitOpen() {
		synchronized (channelHandler) {
			if (channelHandler.getLiveChannelList().isEmpty()) {
				try {
					channelHandler.wait(driver.getConfiguration().getConnectTimeout());
				} catch (InterruptedException e) {
				}
			}
		}
		List<Channel> channelList = channelHandler.getLiveChannelList();
		if (channelList.size() == 0) {
			throw new MongoException("No active connections");
		}
	}
}
