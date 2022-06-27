package cs245.as3.driver;

import java.nio.ByteBuffer;

import cs245.as3.interfaces.LogManager;

/**
 * DO NOT MODIFY THIS FILE IN THIS PACKAGE **
 * Make this an interface
 */
public class LogManagerImpl implements LogManager {
	private ByteBuffer log;
	private int logSize;
	private int logTruncationOffset;
	private int Riop_counter;
	private int Wiop_counter;
	private int nIOSBeforeCrash;
	private boolean serveRequests;

	// 1 GB
	private static final int BUFSIZE_START = 1000000000;

	//
	private static final int RECORD_SIZE = 128;

	public LogManagerImpl() {
		log = ByteBuffer.allocate(BUFSIZE_START);
		logSize = 0;
		serveRequests = true;
	}

	/* **** Public API **** */

	/**
	 * 返回日志在最后一次写入后的偏移
	 * @return
	 */
	public int getLogEndOffset() {
		return logSize;
	}

	/**
	 * 返回偏移为position.长度为size的日志段。超过range之后返回异常
	 * @param position
	 * @param size
	 * @return
	 * @throws ArrayIndexOutOfBoundsException
	 */
	public byte[] readLogRecord(int position, int size) throws ArrayIndexOutOfBoundsException {
		checkServeRequest();
		if ( position < logTruncationOffset || position+size > getLogEndOffset() ) {
			throw new ArrayIndexOutOfBoundsException("Offset " + (position+size) + "invalid: log start offset is " + logTruncationOffset +
					", log end offset is " + getLogEndOffset());
		}

		if ( size > RECORD_SIZE ) {
			throw new IllegalArgumentException("Record length " + size +
					" greater than maximum allowed length " + RECORD_SIZE);
		}

		byte[] ret = new byte[size];
		log.position(position);
		log.get(ret);
		Riop_counter++;
		return ret;
	}

	/**
	 * Returns the length of the log before the append occurs, atomically
	 * 原子追加和持久化日志记录到log尾部，确保前面的追加写已经成功后才执行本次写入
	 * @param record
	 * @return
	 */
	public int appendLogRecord(byte[] record) {
		checkServeRequest();
		if ( record.length > RECORD_SIZE ) {
			throw new IllegalArgumentException("Record length " + record.length +
					" greater than maximum allowed length " + RECORD_SIZE);
		}

		synchronized(this) {
			Wiop_counter++;

			log.position(logSize);

			for ( int i = 0; i < record.length; i++ ) {
				log.put(record[i]);
			}

			int priorLogSize = logSize;

			logSize += record.length;
			return priorLogSize;
		}
	}

	/**
	 * 返回当前truncation的偏移
	 * @return
	 */
	public int getLogTruncationOffset() {
		return logTruncationOffset;
	}

	/**
	 * 从持久化存储中删除偏移为offset之前的日志，将合法起点设置为offset
	 * @param logTruncationOffset
	 */
	public void setLogTruncationOffset(int logTruncationOffset) {
		if (logTruncationOffset > logSize || logTruncationOffset < this.logTruncationOffset) {
			throw new IllegalArgumentException();
		}

		this.logTruncationOffset = logTruncationOffset;
	}


	/* **** For testing only **** */
	
	protected class CrashException extends RuntimeException {
		
	}
	/**
	 * We use this to simulate the log becoming inaccessible after a certain number of operations, 
	 * for the purposes of simulating crashes.
	 * 我们用它来模拟经过一定次数的操作后日志变得不可访问
	 * 用于模拟崩溃
	 */
	private void checkServeRequest() {
		if (nIOSBeforeCrash > 0) {
			nIOSBeforeCrash--;
			if (nIOSBeforeCrash == 0) {
				serveRequests = false;
			}
		}
		if (!serveRequests) {
			//Crash!
			throw new CrashException();
		}
   		assert(!Thread.interrupted()); //Cooperate with timeout:
	}

	protected int getIOPCount() {
		return Riop_counter + Wiop_counter;
	}

	protected void crash() {
		log.clear();
	}

	protected void stopServingRequestsAfterIOs(int nIOsToServe) {
		nIOSBeforeCrash = nIOsToServe;
	}

	protected void resumeServingRequests() {
		serveRequests = true;
		nIOSBeforeCrash = 0;
	}

}
