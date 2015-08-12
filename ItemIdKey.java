package hadoopJoinExample;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;


public class ItemIdKey implements WritableComparable<ItemIdKey>{
	public IntWritable itemId = new IntWritable();
	public IntWritable recordType = new IntWritable();
	public static final IntWritable JOINED2_RECORD = new IntWritable(0);
	public static final IntWritable MATH_RECORD = new IntWritable(1);
	public ItemIdKey(){}
	public ItemIdKey(int itemId, IntWritable recordType) {
	    this.itemId.set(itemId);
	    this.recordType = recordType;
	}
	public void write(DataOutput out) throws IOException {
	    this.itemId.write(out);
	    this.recordType.write(out);
	}

	public void readFields(DataInput in) throws IOException {
	    this.itemId.readFields(in);
	    this.recordType.readFields(in); 
	}
	public int compareTo(ItemIdKey other) {
	    if (this.itemId.equals(other.itemId )) {
	        return this.recordType.compareTo(other.recordType);
	    } else {
	        return this.itemId.compareTo(other.itemId);
	    }
	}
	public boolean equals (ItemIdKey other) {
	    return this.itemId.equals(other.itemId) && this.recordType.equals(other.recordType );
	}

	public int hashCode() {
	    return this.itemId.hashCode();
	}
}
