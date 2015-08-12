package hadoopJoinExample;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;



public class MathRecord implements Writable {
	public IntWritable domainid = new IntWritable();
	public IntWritable clusterid = new IntWritable();
	public IntWritable standardid = new IntWritable();
	public IntWritable itemid = new IntWritable();

	
    public MathRecord(){}              

    public MathRecord(int domainid, int clusterid, int standardid, int itemid) {
    	this.domainid.set(domainid);
    	this.clusterid.set(clusterid);
    	this.standardid.set(standardid);
    	this.itemid.set(itemid);
    	
}

    public void write(DataOutput out) throws IOException {
       
        this.domainid.write(out);
    	this.clusterid.write(out);
    	this.standardid.write(out);
    	this.itemid.write(out);
    	
    }

    public void readFields(DataInput in) throws IOException {
        this.domainid.readFields(in);
        this.clusterid.readFields(in);
        this.standardid.readFields(in);
        this.itemid.readFields(in);
    }
}

