package cs245.as3.interfaces;

import java.util.HashMap;

public interface StorageManager {
	public static class TaggedValue {
		public final long tag;
		public final byte[] value;
		
		public TaggedValue(long tag, byte[] value) {
			this.tag = tag;
			this.value = value;
		}
	}

	/**
	 * Reads the current persisted table of values. Should only be called during recovery.
	 * @return a copy of the data (will be empty if no values are stored)
	 */
	public HashMap<Long, TaggedValue> readStoredTable();

	/**
	 * Queues a write to storage with the given key, tag, and value.
	 * 在数据库中通过key，tag和value查询写操作，tag是一个日志偏移量。
	 * 写不同key时，可能发生持久化顺序和调用queueWrite接口不一致的情况。
	 * 写相同key时，持久化顺序保证和调用顺序一致。你可以认为和每个key有一个独立的队列一样。
	 */
	public void queueWrite(long key, long tag, byte[] value);
}
