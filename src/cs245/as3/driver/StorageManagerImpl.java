package cs245.as3.driver;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import cs245.as3.TransactionManager;
import cs245.as3.driver.LogManagerImpl.CrashException;
import cs245.as3.interfaces.StorageManager;
	
/**
 * DO NOT MODIFY ANY FILE IN THIS PACKAGE **
 * Make this an interface
 */
public class StorageManagerImpl implements StorageManager {
	/**
	 * Stores queued writes for each key.
	 * 存储每个键的写入
	*/
	private final ConcurrentHashMap<Long, StorageManagerEntry> entries;
	/**
	 * Policy to determine which keys not to persist - used for testing purposes
	 * 确定哪些键不需要持久化
	 */
	private long[] dont_persist_keys;
	/**
	 * Set when we are in the middle of recovery just to detect weird calls to readStoredTable.
	 * 当我们处于恢复过程中进行设置，为了检测对readtoredTable的调用
	 */
	protected boolean in_recovery;
	/**
	 * persistedWrite is called on the transaction manager whenever a queued write becomes persistent.
	 * 每当排队写入变成持久化，都会在事务管理器上调用persistedWrite
	 */
	private TransactionManager persistence_listener;
	
	private class StorageManagerEntry {
		/**
		 * latest_version always points to the end of versions
		 * 指向版本的结尾
		 * persisted_version always points to the start of versions
		 * 指向版本的开始
		 */
		volatile TaggedValue latest_version;
		volatile TaggedValue persisted_version;
		ArrayDeque<TaggedValue> versions;
		
		public StorageManagerEntry() {
			versions = new ArrayDeque<>();
			latest_version = null;
			persisted_version = null;
		}
	}
	
	protected StorageManagerImpl() {
		entries = new ConcurrentHashMap<>();
		in_recovery = false;
		dont_persist_keys = null;
	}

	/**
	 * The tag should be a log offset, but how you use it is up to you.
	 * 写不同key时，可能发生持久化顺序和调用queueWrite接口不一致的情况
	 * 写相同key时，持久化顺序保证和调用顺序一致。你可以认为和每个key有一个独立的队列一样
	 * @param key
	 * @param tag
	 * @param value
	 */
	public void queueWrite(long key, long tag, byte[] value) {
		StorageManagerEntry entry = entries.get(key);
		if (entry == null) {
			entry = new StorageManagerEntry();
			StorageManagerEntry prior_entry = entries.putIfAbsent(key, entry);
			if (prior_entry != null) {
				entry = prior_entry;
			}
		}
		TaggedValue tv = new TaggedValue(tag, value);
		synchronized(entry) {
			entry.latest_version = tv;
			entry.versions.add(tv);
		}
	}

	/**
	  * Should only be called during recovery.
	  * 只能在恢复时调用
	  */
	public HashMap<Long, TaggedValue> readStoredTable() {
		if (!in_recovery) {
			throw new RuntimeException("Call to readStoredTable outside of recovery.");
		}
		HashMap<Long, TaggedValue> toRet = new HashMap<>();
		for(Entry<Long, StorageManagerEntry> entry : entries.entrySet()) {
			StorageManagerEntry sme = entry.getValue();
			if (sme.persisted_version != null) {
				toRet.put(entry.getKey(), sme.persisted_version);
			}
		}
		return toRet;
	}
	
	/** ALL METHODS BELOW THIS POINT ARE FOR TESTING PURPOSES **/

	protected void setPersistenceListener(TransactionManager tm) {
		persistence_listener = tm;
	}

	/**
	  * Returns the latest value queued for a particular key. Used only for testing.
	  * 返回特定键队列的最新值。
	  */
	protected TaggedValue readLatestTaggedValue(long key) {
		StorageManagerEntry entry = entries.get(key);
		if (entry != null) {
			TaggedValue latest_version = entry.latest_version;
			return latest_version;
		}
		return null;
	}
	protected byte[] readLatestValue(long key) {
		StorageManagerEntry entry = entries.get(key);
		if (entry != null) {
			TaggedValue latest_version = entry.latest_version;
			return latest_version != null ? latest_version.value : null;
		}
		return null;
	}

	private boolean shouldPersist(long key) {
		if (dont_persist_keys != null) {
			for(long k : dont_persist_keys) {
				if (k == key) {
					return false;
				}
			}
		}
		return true;
	}
	
	//Forget all non-persisted versions. Called only by the driver code to simulate a crash.
	protected void crash() {
		//Break the reference to the persistence listener here:
		persistence_listener = null;
		for(Entry<Long, StorageManagerEntry> entry : entries.entrySet()) {
			StorageManagerEntry sme = entry.getValue();
			sme.versions.clear();
			if (sme.persisted_version != null) {
				//Only remember the persisted version
				sme.versions.add(sme.persisted_version);
			}
			sme.latest_version = sme.persisted_version;
		}
	}
	
	//Persists a particular key. If multiple versions of the value for the key are queued,
	//only the latest queued value is persisted.
	//
	//Returns true if anything needed to be persisted.

	/**
	 * 持久化一个特定的键,如果键的值有多个版本排队,仅保留最新的值
	 * 如果需要持久化，则返回true
	 * @param key
	 * @param entry
	 * @return
	 */
	protected boolean persist(long key, StorageManagerEntry entry) {
		boolean did_persistence = false;
		TaggedValue latest_version = entry.latest_version;
		TaggedValue persisted_version = entry.persisted_version;
		if (latest_version == persisted_version) {
			return did_persistence;
		}
		//Here you can simulate the duration of a writeout, we don't.
		while(true) {
			//Persist each version in the queue in sequence.
			synchronized(entry) {
				if (entry.persisted_version != null && entry.versions.size() == 1) {
					//Nothing to do, everything persisted.
					break;
				}
				if (entry.persisted_version != null) {
					entry.versions.removeFirst();
				}
				entry.persisted_version = entry.versions.getFirst();
			}
			//We just persisted entry.persisted_version.
			//This can only fail due to an error in a test
			//我们只是持久化了entry.persisted_version
			//这只能由于测试中的错误而失败
			if (persistence_listener != null) {
				persistence_listener.writePersisted(key, entry.persisted_version.tag, entry.persisted_version.value);
			}
			did_persistence = true;

	   		assert(!Thread.interrupted()); //Cooperate with timeout:
		}
		return did_persistence;
	}
	
	protected boolean do_persistence_work() {
		boolean did_work = false;
		//Perform a fuzzy checkpoint. Go over all keys and persist while simulating a delay.
		for(Entry<Long, StorageManagerEntry> entry : entries.entrySet()) {
			StorageManagerEntry sme = entry.getValue();
			if (!shouldPersist(entry.getKey())) {
				continue;
			}
			try {
				did_work |= persist(entry.getKey(), sme);
			} catch (CrashException e) {
				//Ignore crash exception.
			}
		}
		return did_work;
	}
	
	protected void blockPersistenceForKeys(long[] keys) {
		dont_persist_keys = keys;
	}
}
