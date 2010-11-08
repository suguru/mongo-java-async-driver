package jp.ameba.mongo;

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
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

/**
 * {@link MongoClient} において接続ごとに生成される
 * {@link ChannelHandler} 実装
 * 
 * @author suguru
 */
public class MongoClientHandler extends SimpleChannelHandler {

	// クライアント用コンテキスト
	private MongoClientContext clientContext;
	
	/**
	 * 
	 * @param clientContext
	 */
	public MongoClientHandler(MongoClientContext clientContext) {
		this.clientContext = clientContext;
	}
	
	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {
	}
	
	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
			throws Exception {
		
		Response response = (Response) e.getMessage();
		if (response != null) {
			RequestFuture future = clientContext.getFuture(response.getHeader().getResponseTo());
			if (future != null) {
				future.setResponse(response);
				future.setDone(true);
				synchronized (future) {
					future.notifyAll();
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
		Request message = (Request) e.getMessage();
		
		RequestFuture future = new RequestFuture(message);
		
		// チャネルを通して送信
		try {
			// バッファを確保
			OutputBuffer outputBuffer = new BasicOutputBuffer();
			BSONEncoder encoder = new BSONEncoder();
			encoder.set(outputBuffer);
			// リクエスト内容を出力
			writeRequest(message, encoder, outputBuffer);
			
			// Safeリクエストの場合は、 getLastError クエリを付加
			Request getLastError = null;
			BSONObject query = message.getSafeLevel().getLastErrorQuery();
			if (query != null) {
				getLastError = new Query(
						message.getDatabaseName(),
						"$cmd",
						0,
						1,
						query,
						null
				);
				writeRequest(getLastError, encoder, outputBuffer);
				future.setGetLastError(getLastError);
			}
			
			// ChannelBuffer を生成
			ChannelBuffer channelBuffer = ChannelBuffers.buffer(outputBuffer.size());
			outputBuffer.pipe(new ChannelBufferOutputStream(channelBuffer));
			
			// ChannelFuture を取得し RequestFuture に設定
			ChannelFuture channelFuture = e.getFuture();
			future.setChannelFuture(channelFuture);
			
			// 送信リエクスト一覧に future を追加
			OperationCode opCode = message.getHeader().getOpCode();
			// safeモード、もしくは返信が見込める場合は、リクエストIDを設定
			if (opCode.hasReply() || getLastError != null) {
				clientContext.addRequest(future);
				if (getLastError == null) {
					message.setWaitingRequestId(message.getRequestId());
				} else {
					message.setWaitingRequestId(getLastError.getRequestId());
				}
			}
			Channels.write(ctx, channelFuture, channelBuffer);
			
		} catch (Exception ex) {
			// 例外が発生してしまった場合は、リクエスト一覧から future を除去
			if (future.getGetLastError() == null) {
				clientContext.removeFuture(future.getRequetId());
			} else {
				clientContext.removeFuture(future.getGetLastError().getRequestId());
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

}
