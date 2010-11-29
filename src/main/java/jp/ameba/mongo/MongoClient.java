package jp.ameba.mongo;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import jp.ameba.mogo.protocol.Delete;
import jp.ameba.mogo.protocol.GetMore;
import jp.ameba.mogo.protocol.Insert;
import jp.ameba.mogo.protocol.KillCursors;
import jp.ameba.mogo.protocol.Message;
import jp.ameba.mogo.protocol.Query;
import jp.ameba.mogo.protocol.Request;
import jp.ameba.mogo.protocol.RequestFuture;
import jp.ameba.mogo.protocol.Response;
import jp.ameba.mogo.protocol.Consistency;
import jp.ameba.mogo.protocol.Update;

import org.bson.BSONObject;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBufferFactory;
import org.jboss.netty.buffer.HeapChannelBufferFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

/**
 * MongoDB との非同期通信を行うクライアント
 * 
 * @author suguru
 */
public class MongoClient {
	
	// Netty用の ChannelFactory
	private ChannelFactory channelFactory;
	// ChannelBufferFactory
	private ChannelBufferFactory channelBufferFactory;
	
	// アクティブなクライアント一覧
	private List<Channel> liveClients;
	// 非アクティブなクライアント一覧
	private List<Channel> deadClients;
	
	// 初期接続先アドレス一覧
	private List<InetSocketAddress> serverAddresses;
	// ラウンドロビン用
	private AtomicInteger roundRobinCounter;
	// クライアント設定
	private MongoClientContext clientContext;
	
	/**
	 * {@link MongoClient} を構成します。
	 */
	public MongoClient() {
		channelFactory = new NioClientSocketChannelFactory(
				Executors.newFixedThreadPool(
						Runtime.getRuntime().availableProcessors(),
						new NamedThreadFactory("mongo-boss-")),
				Executors.newFixedThreadPool(
						Runtime.getRuntime().availableProcessors(),
						new NamedThreadFactory("mongo-worker-"))
		);
		channelBufferFactory = new HeapChannelBufferFactory(ByteOrder.LITTLE_ENDIAN);
		roundRobinCounter = new AtomicInteger();
		clientContext = new MongoClientContext();
	}
	
	/**
	 * ホストアドレス一覧を取得します。
	 * 
	 * @param hostsString
	 * @throws UnknownHostException
	 */
	public void setHosts(String hostsString) throws UnknownHostException {
		
		String[] hostsArray = hostsString.split(",");
		serverAddresses = new ArrayList<InetSocketAddress>(hostsArray.length);
		liveClients = new ArrayList<Channel>(hostsArray.length);
		deadClients = new ArrayList<Channel>(hostsArray.length);
		
		for (int i = 0; i < hostsArray.length; i++) {
			
			String hostLine = hostsArray[i];
			String host = "127.0.0.1";
			int port = 27017;
			
			int colonIndex = hostLine.indexOf(':');
			if (colonIndex >= 0) {
				host = hostLine.substring(0, colonIndex);
				port = Integer.parseInt(hostLine.substring(colonIndex+1));
			} else {
				host = hostLine;
			}
			
			InetSocketAddress address = new InetSocketAddress(host, port);
			serverAddresses.add(address);
		}
	}
	
	/**
	 * MongoDB へ接続します。
	 */
	public void open() {
		MongoFuture future = openAsync();
		try {
			future.await(clientContext.getConfig().getConnectTimeout(), TimeUnit.MILLISECONDS);
		} catch (InterruptedException ex) {
		}
		if (!future.isSuccess()) {
			throw new MongoException("No available MongoDB servers.");
		}
	}
	
	/**
	 * 非同期処理で MongoDB へ接続します。
	 * @return
	 */
	public MongoFuture openAsync() {
		
		MongoFuture mongoFuture = new MongoFuture();
		OpenChannelFutureListener channelFutureListener = new OpenChannelFutureListener(mongoFuture);
		synchronized (channelFutureListener) {
			for (InetSocketAddress address : serverAddresses) {
				ClientBootstrap bootstrap = new ClientBootstrap(channelFactory);
				bootstrap.setPipelineFactory(new MongoPipelineFactory(clientContext));
				ChannelFuture channelFuture = bootstrap.connect(address);
				channelFuture.getChannel().getConfig().setBufferFactory(channelBufferFactory);
				channelFuture.addListener(channelFutureListener);
				channelFutureListener.tryCount++;
			}
		}
		return mongoFuture;
		
	}
	
	/**
	 * MongoDB への接続を閉じます。
	 */
	public void close() {
		MongoFuture future = closeAsync();
		try {
			future.await(clientContext.getConfig().getOperationTimeout(), TimeUnit.MILLISECONDS);
		} catch (InterruptedException ex) {
		}
		if (!future.isSuccess()) {
			throw new MongoException("Failed to close connections");
		}
	}
	
	/**
	 * 非同期処理で MongoDB への接続を閉じます。
	 * @return
	 */
	public MongoFuture closeAsync() {
		MongoFuture mongoFuture = new MongoFuture();
		// 接続中クライアントが存在しない場合は、そのまま返却
		if (liveClients.size() == 0) {
			mongoFuture.setSuccess(true);
			return mongoFuture;
		}
		CloseChannelFutureListener channelFutureListener = new CloseChannelFutureListener(mongoFuture);
		synchronized (channelFutureListener) {
			List<Channel> clients = new ArrayList<Channel>(liveClients);
			for (Channel channel : clients) {
				channelFutureListener.tryCount++;
				ChannelFuture future = channel.close();
				future.addListener(channelFutureListener);
			}
		}
		return mongoFuture;
	}
	
	/**
	 * 接続完了処理のための非同期リスナ
	 */
	private class OpenChannelFutureListener implements ChannelFutureListener {
		private int tryCount = 0;
		private MongoFuture mongoFuture;
		public OpenChannelFutureListener(MongoFuture mongoFuture) {
			this.mongoFuture = mongoFuture;
		}
		public synchronized void operationComplete(ChannelFuture channelFuture) throws Exception {
			if (channelFuture.isSuccess()) {
				// 接続に成功した場合は、アクティブ一覧へ追加
				liveClients.add(channelFuture.getChannel());
			} else {
				// 接続に失敗した場合は、被アクティブ一覧に追加
				deadClients.add(channelFuture.getChannel());
			}
			if (--tryCount <= 0) {
				if (liveClients.size() > 0) {
					// １つでも接続に成功していれば、成功とみなす
					mongoFuture.setSuccess(true);
				}
				// 待ち受けスレッドへ通知
				synchronized (mongoFuture) {
					mongoFuture.notifyAll();
				}
			}
		};
	}
	
	/**
	 * 切断処理のための非同期リスナ
	 * @author suguru
	 */
	private class CloseChannelFutureListener implements ChannelFutureListener {
		private int tryCount = 0;
		private MongoFuture mongoFuture;
		public CloseChannelFutureListener(MongoFuture mongoFuture) {
			this.mongoFuture = mongoFuture;
		}
		@Override
		public synchronized void operationComplete(ChannelFuture channelFuture) throws Exception {
			synchronized (this) {
				if (channelFuture.isSuccess()) {
					liveClients.remove(channelFuture.getChannel());
				} else {
				}
				if (--tryCount <= 0) {
					// アクティブな接続が存在しなくなっていれば、成功とみなす
					if (liveClients.size() == 0) {
						mongoFuture.setSuccess(true);
					}
					// 待ち受けスレッドへ通知
					synchronized (mongoFuture) {
						mongoFuture.notifyAll();
					}
				}
			}
		}
	}
	
	/**
	 * 接続がオープンであるか確認します。
	 * @return
	 */
	public boolean isOpen() {
		return liveClients.size() > 0;
	}

	/**
	 * 更新を実行します。
	 * @param databaseName
	 * @param collectionName
	 * @param selector
	 * @param update
	 * @param upsert
	 * @param multiUpdate
	 * @param consistency
	 */
	public void update(Update update) {
		sendUpdateRequest(update);
	}
	
	/**
	 * 新しいオブジェクトを新規に永続化します。
	 * @param insert
	 */
	public void insert(Insert insert) {
		sendUpdateRequest(insert);
	}
	
	/**
	 * 指定のオブジェクトを削除します。
	 * @param delete
	 */
	public void delete(Delete delete) {
		sendUpdateRequest(delete);
	}

	/**
	 * クエリを送信します。
	 * @param query
	 * @return
	 */
	public Response query(Query query) {
		return sendQueryRequest(query);
	}
	
	/**
	 * カーソルの追加取得を送信します。
	 * @param getMore
	 * @return
	 */
	public Response getMore(GetMore getMore) {
		return sendQueryRequest(getMore);
	}
	
	/**
	 * カーソルを取得します。
	 * @param query
	 * @return
	 */
	public MongoCursor cursor(String databaseName, String collectionName) {
		return new MongoCursor(this, databaseName, collectionName);
	}
	
	/**
	 * カーソルクリアを送信します。
	 * @param kilLCursors
	 */
	public void killCursors(KillCursors killCursors) {
		sendUpdateRequest(killCursors);
	}
	
	/**
	 * 更新系リクエストを送信します。
	 * {@link Consistency} の状況によって、サーバーからの
	 * 返却までの間、処理をブロックします。
	 * 
	 * @param request
	 */
	private void sendUpdateRequest(Request request) {
		Channel channel = getLiveChannel();
		channel.write(request);
		Consistency consistency = request.getConsistency();
		BSONObject getErrorQuery = consistency.getLastErrorQuery();
		if (getErrorQuery != null) {
			waitLastError(request);
		}
	}
	
	/**
	 * クエリリクエストを送信し、レスポンスを取得するまでブロックします。
	 * @param request
	 * @return
	 */
	private Response sendQueryRequest(Request request) {
		Channel channel = getLiveChannel();
		channel.write(request);
		RequestFuture requestFuture = clientContext.getFuture(request.getRequestId());
		try {
			Response response = requestFuture.get();
			if (response.isQueryFailrue()) {
				throw new MongoException("Query failure");
			} else {
				return response;
			}
		} catch (ExecutionException ex) {
			throw new MongoException(ex);
		} catch (InterruptedException ex) {
			throw new MongoException(ex);
		} finally {
			clientContext.removeFuture(request.getRequestId());
		}
	}
	
	/**
	 * 指定のリクエストの getLastError の返りを待ちます。
	 * 
	 * @param request
	 */
	private void waitLastError(Request request) {
		// ResponseFuture を取得
		RequestFuture requestFuture = clientContext.getFuture(request.getWaitingRequestId());
		if (requestFuture != null) {
			try {
				Response response = requestFuture.get();
				BSONObject object = response.getDocuments().get(0);
				// OK でない場合は、例外を発する
				Object ok = object.get("ok");
				boolean isOk = false;
				if (ok.getClass() == Boolean.class) {
					isOk = (Boolean) ok;
				} else if (ok instanceof Number) {
					isOk = ((Number) object.get("ok")).intValue() == 1;
				}
				if (!isOk) {
					String error = (String) object.get("errmsg");
					throw new MongoException(error);
				}
				String err = (String) object.get("err");
				if (err != null) {
					Integer code = (Integer) object.get("code");
					throw new MongoException(err, code);
				}
			} catch (ExecutionException ex) {
				throw new MongoException(ex);
			} catch (InterruptedException ex) {
				throw new MongoException(ex);
			} finally {
				clientContext.removeFuture(request.getWaitingRequestId());
			}
		}
	}
	
	/**
	 * メッセージを送信します。
	 */
	public void message() {
		
		Channel channel = getLiveChannel();
		channel.write(new Message("Hello"));
		
	}
	
	/**
	 * 使用可能な接続を取得します。
	 * 
	 * @return
	 */
	private Channel getLiveChannel() {
		if (liveClients.size() == 0) {
			throw new MongoException("No available connection to mongoDB");
		}
		int rr = Math.abs(roundRobinCounter.getAndIncrement());
		while (true) {
			Channel channel = liveClients.get(rr % liveClients.size());
			if (channel.isConnected()) {
				return channel;
			} else {
				// 接続されていない接続は、除外する
				liveClients.remove(channel);
				deadClients.add(channel);
			}
			if (liveClients.size() == 0) {
				throw new MongoException("No available connection to mongoDB");
			}
		}
	}

	/**
	 * MongoDB用の接続パイプラインハンドラ
	 * @author suguru
	 */
	private static class MongoPipelineFactory implements ChannelPipelineFactory {
		private MongoClientContext clientContext;
		public MongoPipelineFactory(MongoClientContext clientContext) {
			this.clientContext = clientContext;
		}
		@Override
		public ChannelPipeline getPipeline() throws Exception {
			return Channels.pipeline(
					new MongoDecoder(),
					new MongoClientHandler(clientContext)
			);
		}
	}
	
	/**
	 * {@link NamedThreadFactory}
	 * @author suguru
	 */
	private static class NamedThreadFactory implements ThreadFactory {
		// 連番用
		private AtomicInteger id;
		// ベースとなるプリフィクス
		private String name;
		
		private NamedThreadFactory(String name) {
			this.id = new AtomicInteger();
			this.name = name;
		}
		
		@Override
		public Thread newThread(Runnable r) {
			Thread thread = new Thread(r);
			thread.setDaemon(true);
			thread.setName(name + id.incrementAndGet());
			return thread;
		}
	}
}
