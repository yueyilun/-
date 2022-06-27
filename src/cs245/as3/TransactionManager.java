package cs245.as3;

import java.nio.ByteBuffer;
import java.util.*;

import cs245.as3.interfaces.LogManager;
import cs245.as3.interfaces.StorageManager;
import cs245.as3.interfaces.StorageManager.TaggedValue;

/**
 * You will implement this class.
 * <p>
 * The implementation we have provided below performs atomic transactions but the changes are not durable.
 * Feel free to replace any of the data structures in your implementation, though the instructor solution includes
 * the same data structures (with additional fields) and uses the same strategy of buffering writes until commit.
 * <p>
 * Your implementation need not be threadsafe, i.e. no methods of TransactionManager are ever called concurrently.
 * <p>
 * You can assume that the constructor and initAndRecover() are both called before any of the other methods.
 */

public class TransactionManager {
    /**
     * 写入数据键值对
     */
    class WritesetEntry {
        public long key;
        public byte[] value;

        public WritesetEntry(long key, byte[] value) {
            this.key = key;
            this.value = value;
        }
    }

    /**
     * Holds the latest value for each key.
     * 每个键最近的值
     */
    private HashMap<Long, TaggedValue> latestValues;
    /**
     * Hold on to writesets until commit.
     * 存储提交之前的写操作
     */
    private HashMap<Long, ArrayList<WritesetEntry>> writesets;

    /**
     * 事务集合
     */
    private HashMap<Long, ArrayList<LogRecords>> transactionsets;

    /**
     * 日志控制器
     */
    private LogManager logManager;
    /**
     * 存储控制器
     */
    private StorageManager storageManager;

    /**
     * 定义工具类
     */
    private LogUtils util;

    /**
     * 单条日志最大长度
     */
    public final static int maxLength = 128;
    /**
     * 日志起始位置
     */
    public final static int begin = 16;

    /**
     * 存储日志大小的小根堆
     */
    private PriorityQueue<Long> pq;

    public TransactionManager() {
        writesets = new HashMap<>();
        //see initAndRecover
        latestValues = null;
        util = new LogUtils();
        pq = new PriorityQueue<>();
        transactionsets = new HashMap<Long, ArrayList<LogRecords>>();
    }

    /**
     * Prepare the transaction manager to serve operations.
     * At this time you should detect whether the StorageManager is inconsistent and recover it.
     */
    public void initAndRecover(StorageManager sm, LogManager lm) {
        this.storageManager = sm;
        this.logManager = lm;
        latestValues = sm.readStoredTable();
        // 日志列表
        ArrayList<LogRecords> logRecordsList = new ArrayList<>();

        // 提交序号
        HashSet<Long> commitIdx = new HashSet<Long>();
        ArrayList<Integer> commitIdxList = new ArrayList<>();
        // 从日志当前偏移量到日志结尾，磁盘中读取数据
        for (int idx = lm.getLogTruncationOffset(); idx < lm.getLogEndOffset(); ) {
            // 读取当前位置的字节数组
            // 每次读取16字节大小的日志
            byte[] bytes = lm.readLogRecord(idx, begin);
            int size = util.getLogSize(bytes);
            ByteBuffer buffer = ByteBuffer.allocate(size);
            // 往内存中添加数据
            for (int i = 0; i < size; i += maxLength) {
                int l = Math.min(size-i,maxLength);
                buffer.put(lm.readLogRecord(idx+i,l));
            }
            byte[] recordBytes = buffer.array();
            // 将字节数组转换成日志记录
            LogRecords record = LogRecords.changeToLogRecord(recordBytes);
            logRecordsList.add(record);
            // 提交日志
            if (record.getType() == 2) {
                commitIdx.add(record.getTxID());
            }
            idx += record.size();
            commitIdxList.add(idx);
        }

        // 持久化：对提交的事务的日志重做，并持久化写入
        Iterator<Integer> iterator = commitIdxList.iterator();
        for (LogRecords lr : logRecordsList) {
            if(commitIdx.contains(lr.getTxID()) && lr.getType() == 1){
                long tag = iterator.next();
                pq.add(tag);
                latestValues.put(lr.getKey(), new TaggedValue(tag,lr.getValue()));
                sm.queueWrite(lr.getKey(), tag, lr.getValue());
            }
        }
    }

    /**
     * Indicates the start of a new transaction. We will guarantee that txID always increases (even across crashes)
     * 为开启一个新的事务，接口需要确保分配的txID单调递增（即使在系统crash后，仍需满足该特性）
     */
    public void start(long txID) {
        // TODO: Not implemented for non-durable transactions, you should implement this
        // 往事务集合中添加一条日志记录集合
        transactionsets.put(txID, new ArrayList<LogRecords>());
    }

    /**
     * Returns the latest committed value for a key by any transaction.
     * 返回任意事务最近提交的key的value
     */
    public byte[] read(long txID, long key) {
        TaggedValue taggedValue = latestValues.get(key);
        return taggedValue == null ? null : taggedValue.value;
    }

    /**
     * Indicates a write to the database. Note that such writes should not be visible to read()
     * calls until the transaction making the write commits. For simplicity, we will not make reads
     * to this same key from txID itself after we make a write to the key.
     * 将数据写入数据库，需保证在该事务提交前，数据不能被read()接口读到。为了简化场景，不考虑同一个事务内
     * 写了一个key，在读取相同key的情况
     */
    public void write(long txID, long key, byte[] value) {
        // 根据事务ID获取写集合
        // get函数：获取指定 key 对应对 value
        ArrayList<WritesetEntry> writeset = writesets.get(txID);
        if (writeset == null) {
            writeset = new ArrayList<>();
            writesets.put(txID, writeset);
        }
        // 添加一个写操作
        writeset.add(new WritesetEntry(key, value));
        // 创建一个新的日志，类型为写操作
        LogRecords currentLogRecords = new LogRecords(1, txID, key, value);
        // 追加日志长度
        currentLogRecords.setSize(16 + 8 + value.length);
        // 把日志添加到当前事务集合
        transactionsets.get(txID).add(currentLogRecords);
    }

    /**
     * Commits a transaction, and makes its writes visible to subsequent read operations.\
     * 提交一个事务，使该事务写入的数据被后续的read()操作读取
     */
    public void commit(long txID) {
        // 创建提交日志，并添加到事务集合中
        LogRecords commitRecord = new LogRecords(2, txID, -1, null);
        transactionsets.get(txID).add(commitRecord);

        // 存储键和日志大小
        HashMap<Long, Long> tempKey = new HashMap<Long, Long>();
        for (LogRecords record : transactionsets.get(txID)) {
            // 如果是写操作的话,添加到集合中
            if (record.getType() == 1) {
                long logSize = 0;
                // 将日志转换为byte数组，写磁盘
                byte[] bytes = LogRecords.changeToByte(record);
                int size = record.size();
                // 将字节数组保存到缓冲区
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                for (int i = 0; i < size; i += maxLength) {
                    int length = Math.min(size - i, maxLength);
                    byte[] temp = new byte[length];
                    //从数组i下标开始（包含），读取length长度缓存到数组temp
                    buffer.get(temp, i, length);
                    logSize = logManager.appendLogRecord(temp);
                }
                tempKey.put(record.getKey(), logSize);
            } else {
                //否则的话直接追加LogRecord，提交等操作
                byte[] bytes = LogRecords.changeToByte(record);
                int size = record.size();
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                for (int i = 0; i < size; i += maxLength) {
                    int l = Math.min(size - i, maxLength);
                    byte[] temp = new byte[l];
                    buffer.get(temp, i, l);
                    logManager.appendLogRecord(temp);
                }
            }
        }
        ArrayList<WritesetEntry> writeset = writesets.get(txID);
        if (writeset != null) {
            for (WritesetEntry x : writeset) {
                // tag is unused in this implementation:
                // 根据日志偏移添加到lastestValues
                long tag = tempKey.get(x.key);
                latestValues.put(x.key, new TaggedValue(tag, x.value));
                // 存储日志位置
                pq.add(tag);
                // 将kv存储到数据库当中
                storageManager.queueWrite(x.key, tag, x.value);
            }
            // 提交后移除
            writesets.remove(txID);
        }
    }

    /**
     * Aborts a transaction.
     * 回滚事务
     */
    public void abort(long txID) {
        writesets.remove(txID);
        transactionsets.remove(txID);
    }

    /**
     * The storage manager will call back into this procedure every time a queued write becomes persistent.
     * These calls are in order of writes to a key and will occur once for every such queued write, unless a crash occurs.
     */
    public void writePersisted(long key, long persisted_tag, byte[] persisted_value) {
        // 这里可以根据redo log的实现来处理
        //当checkpoint和write pos相遇，表示redo log已经满了，这时数据库停止更新数据库更新语句的执行，转而进行redo log日志同步到磁盘中
        if (persisted_tag==pq.peek()){
            // 设置checkpoint是否为当前最早的日志
            logManager.setLogTruncationOffset((int)persisted_tag);
        }
        pq.remove(persisted_tag);
    }
}
