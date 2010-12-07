package jp.ameba.mongo;

import java.net.SocketAddress;
import java.nio.ByteOrder;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.netty.buffer.ChannelBufferFactory;
import org.jboss.netty.buffer.HeapChannelBufferFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

/**
 * {@link MongoConnection} を生成・取得するための
 * ドライバークラスです。
 * Sharding 分散された接続のための分散接続をサポートします。
 * 
 * @author suguru
 */
public class MongoDriver {
	
	// Netty用の ChannelFactory
	private ChannelFactory channelFactory;
	// ChannelBufferFactory
	private ChannelBufferFactory channelBufferFactory;
	// ChannelPipeline Factory
	private ChannelPipelineFactory channelPipelineFactory;
	// Client Handler
	private MongoChannelHandler mongoClientHandler;
	// Configuration
	private MongoConfiguration mongoConfig;
	
	public MongoDriver() {
		this(new MongoChannelHandler());
	}
	
	public MongoDriver(MongoChannelHandler channelHandler) {
		this.channelFactory = new NioClientSocketChannelFactory(
				Executors.newFixedThreadPool(
						Runtime.getRuntime().availableProcessors(),
						new NamedThreadFactory("mongo-boss-")),
				Executors.newFixedThreadPool(
						Runtime.getRuntime().availableProcessors(),
						new NamedThreadFactory("mongo-worker-"))
		);
		this.channelBufferFactory = new HeapChannelBufferFactory(ByteOrder.LITTLE_ENDIAN);
		this.channelPipelineFactory = new MongoPipelineFactory();
		this.mongoClientHandler = channelHandler;
		this.mongoConfig = new MongoConfiguration();
	}
	
	/**
	 * MongoDB の接続オブジェクトを作成します。
	 * @param host
	 * @return
	 */
	public MongoConnection createConnection(SocketAddress socketAddress) {
		try {
			Channel channel = channelFactory.newChannel(channelPipelineFactory.getPipeline());
			channel.getConfig().setBufferFactory(channelBufferFactory);
			MongoConnection connection = new MongoConnectionImpl(channel, socketAddress, mongoConfig);
			return connection;
		} catch (MongoException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new MongoException(ex);
		}
	}
	
	/**
	 * MongoDB の設定を取得します。
	 * @return
	 */
	public MongoConfiguration getConfiguration() {
		return mongoConfig;
	}
	
	/**
	 * MongoDB用の接続パイプラインハンドラ
	 * @author suguru
	 */
	private class MongoPipelineFactory implements ChannelPipelineFactory {
		
		private MongoDecoder mongoDecoder;
		
		public MongoPipelineFactory() {
			mongoDecoder = new MongoDecoder();
		}
		
		@Override
		public ChannelPipeline getPipeline() throws Exception {
			return Channels.pipeline(mongoDecoder, mongoClientHandler);
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
