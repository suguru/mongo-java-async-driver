package jp.ameba.mongo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import jp.ameba.mogo.protocol.MessageHeader;
import jp.ameba.mogo.protocol.OperationCode;
import jp.ameba.mogo.protocol.Query;
import jp.ameba.mogo.protocol.Request;
import jp.ameba.mogo.protocol.RequestFuture;
import jp.ameba.mogo.protocol.Response;

import org.bson.BSONEncoder;
import org.bson.BSONObject;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.OutputBuffer;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

/**
 * MongoDB プロトコルを実装した
 * {@link ChannelHandler} 実装クラスです。
 * 
 * @author suguru
 */
public class MongoChannelHandler extends SimpleChannelHandler {

	// アクティブなチャネル一覧
	private ConcurrentMap<Integer, Channel> liveChannelMap;
	
	// アクティブなチャネル一覧
	private List<Channel> liveChannelList;
	
	// 非アクティブなチャネル一覧
	private ConcurrentMap<Integer, Channel> deadChannelMap;
	
	// 返答待ち中の送信済みリクエスト一覧
	private ConcurrentMap<Integer, Request> requestMap;
	
	/**
	 * {@link MongoChannelHandler}
	 * @param clientContext
	 */
	public MongoChannelHandler() {
		this.liveChannelList = new ArrayList<Channel>();
		this.liveChannelMap = new ConcurrentHashMap<Integer, Channel>();
		this.deadChannelMap = new ConcurrentHashMap<Integer, Channel>();
		this.requestMap = new ConcurrentHashMap<Integer, Request>();
	}
	
	/**
	 * アクティブな接続チャネル一覧を取得します。
	 * @return
	 */
	public List<Channel> getLiveChannelList() {
		return liveChannelList;
	}
	
	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {
		Channel channel = ctx.getChannel();
		liveChannelMap.put(channel.getId(), channel);
		deadChannelMap.remove(channel.getId());
		synchronized (this) {
			List<Channel> channelList = new ArrayList<Channel>(liveChannelMap.values());
			this.liveChannelList = channelList;
			notifyAll();
		}
	}
	
	@Override
	public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {
		Channel channel = ctx.getChannel();
		liveChannelMap.remove(channel.getId());
		deadChannelMap.put(channel.getId(), channel);
		synchronized (this) {
			List<Channel> channelList = new ArrayList<Channel>(liveChannelMap.values());
			this.liveChannelList = channelList;
			notifyAll();
		}
	}
	
	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
			throws Exception {
		
		Response response = (Response) e.getMessage();
		if (response != null) {
			Request request = requestMap.remove(response.getHeader().getResponseTo());
			if (request != null) {
				RequestFuture future = request.getFuture();
				if (future != null) {
					future.setResponse(response);	
					future.setDone(true);
					synchronized (future) {
						future.notifyAll();	
					}
				}
			}
		}
		
	}
	
	@Override
	public void writeRequested(
			ChannelHandlerContext ctx,
			MessageEvent e)
				throws Exception {
		
		// リクエストをバッファに変換して送信
		Request request = (Request) e.getMessage();
		RequestFuture future = new RequestFuture(request);
		request.setFuture(future);
		
		// チャネルを通して送信
		try {
			// バッファを確保
			OutputBuffer outputBuffer = new BasicOutputBuffer();
			BSONEncoder encoder = new BSONEncoder();
			encoder.set(outputBuffer);
			// リクエスト内容を出力
			writeRequest(request, encoder, outputBuffer);
			
			// Safeリクエストの場合は、 getLastError クエリを付加
			Request getLastError = null;
			BSONObject query = request.getConsistency().getLastErrorQuery();
			if (query != null) {
				getLastError = new Query(
						request.getDatabaseName(),
						"$cmd",
						0,
						1,
						query,
						null
				);
				getLastError.setFuture(future);
				writeRequest(getLastError, encoder, outputBuffer);
			}
			
			// ChannelBuffer を生成
			ChannelBuffer channelBuffer = ChannelBuffers.buffer(outputBuffer.size());
			outputBuffer.pipe(new ChannelBufferOutputStream(channelBuffer));
			
			// ChannelFuture を取得し RequestFuture に設定
			ChannelFuture channelFuture = e.getFuture();
			future.setChannelFuture(channelFuture);
			
			// 送信リエクスト一覧に future を追加
			OperationCode opCode = request.getHeader().getOpCode();
			// safeモード、もしくは返信が見込める場合は、リクエストIDを設定
			if (opCode.hasReply() || getLastError != null) {
				if (getLastError == null) {
					requestMap.put(future.getRequetId(), request);
					request.setWaitingRequestId(request.getRequestId());
				} else {
					requestMap.put(getLastError.getRequestId(), request);
					request.setWaitingRequestId(getLastError.getRequestId());
				}
			}
			Channels.write(ctx, channelFuture, channelBuffer);
			
		} catch (Exception ex) {
			// 例外が発生してしまった場合は、リクエスト一覧から future を除去
			if (future.getGetLastError() == null) {
				requestMap.remove(future.getRequetId());
			} else {
				requestMap.remove(future.getGetLastError().getRequestId());
			}
			throw ex;
		}
	}
	
	/**
	 * 
	 * @param request
	 * @param encoder
	 * @param output
	 */
	private void writeRequest(
			Request request,
			BSONEncoder encoder,
			OutputBuffer output) {
		// 開始位置を保持
		int start = output.getPosition();
		// ヘッダ領域を確保
		output.setPosition(start + 16);
		// リクエスト内容を出力
		request.encode(encoder);
		// 最終位置からサイズを特定
		int end = output.getPosition();
		int size = end - start;
		// ヘッダにサイズを設定
		MessageHeader header = request.getHeader();
		header.setMessageLength(size);
		// 開始位置まで戻る
		output.setPosition(start);
		// ヘッダ内容を出力
		header.encode(output);
		// 最終位置に戻る
		output.setPosition(end);
	}

	/*
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
			throws Exception {
	}
	*/
}
