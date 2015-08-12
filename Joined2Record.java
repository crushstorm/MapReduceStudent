package hadoopJoinExample;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Joined2Record implements Writable {
	public IntWritable studentid = new IntWritable();
	public Text id = new Text();
	public IntWritable score = new IntWritable();
	public Text term = new Text();
	public IntWritable coreid = new IntWritable();
	public Text country = new Text();
	public IntWritable stateid = new IntWritable();
	public Text districtid = new Text();
	public Text schoolid = new Text();
	public Text sectionid = new Text();
	
    public Joined2Record(){}

    public Joined2Record(int studentid, String id, int score, String term, int coreid, String country, int stateid, String districtid, 
			String schoolid, String sectionid){ 
    	this.studentid.set(studentid);
    	this.id.set(id);
    	this.score.set(score);
    	this.term.set(term);
    	this.coreid.set(coreid);
    	this.country.set(country);
    	this.districtid.set(districtid);
    	this.schoolid.set(schoolid);
    	this.sectionid.set(sectionid);
    	this.stateid.set(stateid);
    }
    
          
    public void write(DataOutput out) throws IOException {
        this.id.write(out);
        this.studentid.write(out);
        this.coreid.write(out);
        this.term.write(out);
        this.score.write(out);
        this.country.write(out);
        this.districtid.write(out);
        this.sectionid.write(out);
        this.stateid.write(out);
        this.schoolid.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        this.id.readFields(in);
        this.studentid.readFields(in);
        this.coreid.readFields(in);
        this.term.readFields(in);
        this.score.readFields(in);
        this.country.readFields(in);
        this.districtid.readFields(in);
        this.sectionid.readFields(in);
        this.stateid.readFields(in);
        this.schoolid.readFields(in);
    }
}

