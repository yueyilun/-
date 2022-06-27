package cs245.as3;

import java.nio.ByteBuffer;

/**
 * 日志记录
 */
public class LogRecords {

    /**
     * 1字节
     * 0：开始事务
     * 1：写事务
     * 2：提交事务
     * 3：回滚
     */
    private int type;

    // 事务ID,8字节
    private long txID;

    // 键,8字节
    private long key;

    // 值
    private byte[] value;

    // 日志大小 type+txID+size+key+value=4+8+4+8+x
    private int size;

    public LogRecords(int type, long txID, long key, byte[] value) {
        this.type = type;
        this.txID = txID;
        this.key = key;
        this.value = value;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public long getTxID() {
        return txID;
    }

    public void setTxID(long txID) {
        this.txID = txID;
    }

    public long getKey() {
        return key;
    }

    public void setKey(long key) {
        this.key = key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public int size() {
        size = 16;
        if (type == 1) {
            size = size + 8 + value.length;
        }
        return size;
    }

    /**
     * 将日志转换成byte[]
     */
    public static byte[] changeToByte(LogRecords logRecords) {
        // 分配内存
        ByteBuffer record = ByteBuffer.allocate(128);
        record.putLong(logRecords.txID);
        record.putInt(logRecords.type);
        record.putInt(logRecords.size());
        if (logRecords.type == 1) {
            record.putLong(logRecords.key);
            record.put(logRecords.value);
        }
        byte[] result = record.array();
        return result;
    }

    /**
     * 将byte[]转换成日志
     */
    public static LogRecords changeToLogRecord(byte[] recordBytes) {
        ByteBuffer buff = ByteBuffer.wrap(recordBytes);
        long id=buff.getLong();
        LogRecords record=new LogRecords(buff.getInt(),id,-1,null);
        record.size=buff.getInt();
        if(record.type==1){
            record.key=buff.getLong();
            int size=record.size-24;
            record.value=new byte[size];
            for(int i=0;i<size;i++){
                record.value[i]=buff.get();
            }
        }
        return record;
    }
}