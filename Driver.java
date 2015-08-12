package hadoopJoinExample;
import java.io.File;
import java.io.IOException; 
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.NullWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.io.Writable; 
import org.apache.hadoop.io.WritableComparable; 
import org.apache.hadoop.io.WritableComparator; 
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.Mapper; 
import org.apache.hadoop.mapreduce.Reducer; 
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat; 
import org.apache.hadoop.util.GenericOptionsParser; 
import org.apache.hadoop.util.ToolRunner;


public class Driver extends org.apache.hadoop.conf.Configured implements org.apache.hadoop.util.Tool{
	public static class JoinGroupingComparator extends WritableComparator {
		public JoinGroupingComparator() {
	       super (ItemIdKey.class, true);
	    }                          
		 @Override
		    public int compare (WritableComparable a, WritableComparable b){
		        ItemIdKey first = (ItemIdKey) a;
		        ItemIdKey second = (ItemIdKey) b;
		        return first.itemId.compareTo(second.itemId);
		                      
		    }
	}
	public static class JoinSortingComparator extends WritableComparator {
	    public JoinSortingComparator()
	    {
	        super (ItemIdKey.class, true);
	    }
	                               
	    @Override
	    public int compare (WritableComparable a, WritableComparable b){
	        ItemIdKey first = (ItemIdKey) a;
	        ItemIdKey second = (ItemIdKey) b;
	                                 
	        return first.compareTo(second);
	    }
	}
	
	public static class Joined2Mapper extends Mapper<LongWritable, Text, ItemIdKey, JoinGenericWritable>{
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {                           
	        
	    	String[] recordFields = value.toString().split(",");
	        int studentid = Integer.parseInt(recordFields[0]);
	        String id =  recordFields[1];
	        int itemid = Integer.parseInt(recordFields[2]);
	        int score = Integer.parseInt(recordFields[3]);
	        String term = recordFields[4];
	        int coreid = Integer.parseInt(recordFields[5]);
	        String country = recordFields[6];
	        int stateid = Integer.parseInt(recordFields[7]);
	        String districtid = recordFields[8];
	        String schoolid = recordFields[9];
	        String sectionid =recordFields[10];
	        
	        ItemIdKey recordKey = new ItemIdKey(itemid, ItemIdKey.JOINED2_RECORD);
	        Joined2Record record = new Joined2Record (studentid, id,score, term, coreid, country, stateid, districtid, 
	    			 schoolid, sectionid);
	                                               
	        JoinGenericWritable genericRecord = new JoinGenericWritable(record);
	        context.write(recordKey, genericRecord);
	    }
	}
	         
	
	//int domainid, int clusterid, int standardid, int itemid)
	
	public static class MathMapper extends Mapper<LongWritable, Text, ItemIdKey, JoinGenericWritable>{
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String[] recordFields = value.toString().split(",");
	        int domainid = Integer.parseInt(recordFields[0]);
	        int clusterid = Integer.parseInt(recordFields[1]);
	        int standardid = Integer.parseInt(recordFields[2]);
	        int itemid = Integer.parseInt(recordFields[3]);
                    
	        ItemIdKey recordKey = new ItemIdKey(itemid, ItemIdKey.MATH_RECORD);
	        MathRecord record = new MathRecord(domainid, clusterid, standardid, itemid);
	        JoinGenericWritable genericRecord = new JoinGenericWritable(record);
	        context.write(recordKey, genericRecord);
	    }
	}
	
	public static class JoinRecuder extends Reducer<ItemIdKey, JoinGenericWritable, NullWritable, Text>{
	    public void reduce(ItemIdKey key, Iterable<JoinGenericWritable> values, Context context) throws IOException, InterruptedException{
	        StringBuilder output = new StringBuilder();
	        List<String> mylist= new ArrayList<String>();
	        
	        for (JoinGenericWritable v : values) {
	            Writable record = v.get();
	            if (key.recordType.equals( ItemIdKey.MATH_RECORD)){
	            	 MathRecord pRecord = (MathRecord)record;
	            	 output.append(Integer.parseInt(key.itemId.toString())).append(",");
	            	 output.append(pRecord.domainid.toString()).append(",");
	            	 output.append(pRecord.clusterid.toString()).append(",");
	            	 output.append(pRecord.standardid.toString());
	            } else {
	            	StringBuilder output2 = new StringBuilder();	
	            	Joined2Record record2 = (Joined2Record)record;
	                output2.append(record2.studentid.toString()).append(",");
	                output2.append(record2.id).append(",");
	                output2.append(record2.score).append(",");
	                output2.append(record2.term).append(",");
	                output2.append(record2.coreid.toString()).append(",");
	                output2.append(record2.country).append(",");
	                output2.append(record2.stateid.toString()).append(",");
	                output2.append(record2.districtid).append(",");
	                output2.append(record2.schoolid).append(",");
	                output2.append(record2.sectionid).append(",");

	                mylist.add(output2.toString());
	            }
	        }
	        	for(int i=0;i<mylist.size();i++)
	 		       context.write(NullWritable.get(), new Text(mylist.get(i) + output.toString()));
	    }
	}
	
	public int run(String[] allArgs) throws Exception {
	    String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();
	                               
	    Job job = Job.getInstance(getConf());
	    job.setJarByClass(Driver.class);
	                               
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	                               
	    job.setMapOutputKeyClass(ItemIdKey.class);
	    job.setMapOutputValueClass(JoinGenericWritable.class);
	                               
	    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Joined2Mapper.class);
	    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MathMapper.class);
	                              
	    job.setReducerClass(JoinRecuder.class);
	                         
	    job.setSortComparatorClass(JoinSortingComparator.class);
	    job.setGroupingComparatorClass(JoinGroupingComparator.class);
	                               
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	                               
	    FileOutputFormat.setOutputPath(job, new Path(allArgs[2]));
	    boolean status = job.waitForCompletion(true);
	    if (status) {
	        return 0;
	    } else {
	        return 1;
	    }             
	}


	public static void main(String[] args) throws Exception {
        FileUtils.deleteDirectory(new File("/home/cloudera/output"));
		Configuration conf = new Configuration();
	    int res = ToolRunner.run(new Driver(), args);
	}
}
