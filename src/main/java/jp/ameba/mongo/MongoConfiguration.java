package jp.ameba.mongo;

import jp.ameba.mogo.protocol.Consistency;

/**
 * {@link MongoConnection} の各種設定
 * 
 * @author suguru
 */
public class MongoConfiguration {

	// 接続時タイムアウト
	private long connectTimeout = 10000L;
	// 各種処理タイムアウト
	private long operationTimeout = 10000L;
	// デフォルトの一貫性レベル
	private Consistency defaultConsistency = Consistency.SAFE;

	public MongoConfiguration() {
	}
	
	/**
	 * 接続タイムアウト時間を取得します。
	 * @return
	 */
	public long getConnectTimeout() {
		return connectTimeout;
	}
	
	/**
	 * 操作タイムアウト時間を取得します。
	 * @return
	 */
	public long getOperationTimeout() {
		return operationTimeout;
	}
	
	/**
	 * デフォルトの一貫性レベルを取得します。
	 * @return
	 */
	public Consistency getDefaultConsistency() {
		return defaultConsistency;
	}
	
	/**
	 * 接続タイムアウトを設定します。
	 * @param connectTimeout
	 */
	public void setConnectTimeout(long connectTimeout) {
		this.connectTimeout = connectTimeout;
	}
	
	/**
	 * 操作タイムアウトを設定します。
	 * @param operationTimeout
	 */
	public void setOperationTimeout(long operationTimeout) {
		this.operationTimeout = operationTimeout;
	}

	/**
	 * デフォルトの一貫性レベルを設定します。
	 * @param defaultConsistency
	 */
	public void setDefaultConsistency(Consistency defaultConsistency) {
		this.defaultConsistency = defaultConsistency;
	}
}
