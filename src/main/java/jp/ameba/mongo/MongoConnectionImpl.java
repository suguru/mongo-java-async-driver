package jp.ameba.mongo;

import java.net.SocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import jp.ameba.mogo.protocol.Consistency;
import jp.ameba.mogo.protocol.Delete;
import jp.ameba.mogo.protocol.GetMore;
import jp.ameba.mogo.protocol.Insert;
import jp.ameba.mogo.protocol.KillCursors;
import jp.ameba.mogo.protocol.Query;
import jp.ameba.mogo.protocol.Request;
import jp.ameba.mogo.protocol.RequestFuture;
import jp.ameba.mogo.protocol.Response;
import jp.ameba.mogo.protocol.Update;

import org.bson.BSONObject;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

/**
 * MongoDB との非同期通信を行うクライアント
 * 
 * @author suguru
 */
public class MongoConnectionImpl implements MongoConnection {
	
	// Netty用の ChannelFactory
	private Channel channel;
	
	// 初期接続先アドレス一覧
	private SocketAddress serverAddress;
	
	// 接続設定
	private MongoConfiguration connectionConfig;
	
	/**
	 * {@link MongoConnectionImpl} を構成します。
	 */
	public MongoConnectionImpl(
			Channel channel,
			SocketAddress serverAddress,
			MongoConfiguration connectionConfig) {
		this.channel = channel;
		this.serverAddress = serverAddress;
		this.connectionConfig = connectionConfig;
	}
	
	@Override
	public int getChannelId() {
		return channel.getId();
	}
	
	/**
	 * MongoDB へ接続します。
	 */
	public void open() {
		MongoFuture future = openAsync();
		try {
			future.await(connectionConfig.getConnectTimeout(), TimeUnit.MILLISECONDS);
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
		ChannelFuture channelFuture = channel.connect(serverAddress);
		channelFuture.addListener(new OpenChannelFutureListener(mongoFuture));
		return mongoFuture;
	}
	
	/**
	 * MongoDB への接続を閉じます。
	 */
	public void close() {
		MongoFuture future = closeAsync();
		try {
			future.await(connectionConfig.getOperationTimeout(), TimeUnit.MILLISECONDS);
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
		CloseChannelFutureListener channelFutureListener = new CloseChannelFutureListener(mongoFuture);
		ChannelFuture channelFuture = channel.close();
		channelFuture.addListener(channelFutureListener);
		return mongoFuture;
	}
	
	/**
	 * 接続完了処理のための非同期リスナ
	 */
	private class OpenChannelFutureListener implements ChannelFutureListener {
		private MongoFuture mongoFuture;
		public OpenChannelFutureListener(MongoFuture mongoFuture) {
			this.mongoFuture = mongoFuture;
		}
		public synchronized void operationComplete(ChannelFuture channelFuture) throws Exception {
			if (channelFuture.isSuccess()) {
				// １つでも接続に成功していれば、成功とみなす
				mongoFuture.setSuccess(true);
			}
			// 待ち受けスレッドへ通知
			synchronized (mongoFuture) {
				mongoFuture.notifyAll();
			}
		};
	}
	
	/**
	 * 切断処理のための非同期リスナ
	 * @author suguru
	 */
	private class CloseChannelFutureListener implements ChannelFutureListener {
		private MongoFuture mongoFuture;
		public CloseChannelFutureListener(MongoFuture mongoFuture) {
			this.mongoFuture = mongoFuture;
		}
		@Override
		public synchronized void operationComplete(ChannelFuture channelFuture) throws Exception {
			synchronized (this) {
				if (channelFuture.isSuccess()) {
					mongoFuture.setSuccess(true);
				}
				// 待ち受けスレッドへ通知
				synchronized (mongoFuture) {
					mongoFuture.notifyAll();
				}
			}
		}
	}
	
	/**
	 * 接続がオープンであるか確認します。
	 * @return
	 */
	public boolean isOpen() {
		return channel.isOpen();
	}
	
	/**
	 * 接続中であるか確認します。
	 * @return
	 */
	public boolean isConnected() {
		return channel.isConnected();
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
		channel.write(request);
		RequestFuture requestFuture = request.getFuture();
		try {
			Response response = requestFuture.get();
			if (response.isQueryFailrue()) {
				throw new MongoException("Query failure " + response.getErrorMessage());
			} else {
				return response;
			}
		} catch (ExecutionException ex) {
			throw new MongoException(ex);
		} catch (InterruptedException ex) {
			throw new MongoException(ex);
		}
	}
	
	/**
	 * 指定のリクエストの getLastError の返りを待ちます。
	 * 
	 * @param request
	 */
	private void waitLastError(Request request) {
		// ResponseFuture を取得
		RequestFuture requestFuture = request.getFuture();
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
			}
		}
	}
	
	@Override
	protected void finalize() throws Throwable {
		closeAsync();
	}
}
