package hadoop.TianChiMapreduce.GetUserFeature;

import java.awt.List;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.TaskContext;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

public class GetTwoSongCommonUser {

  public static class TokenizerMapper extends MapperBase {
    private Record key;
    private Record value;
    
    @Override
    public void setup(TaskContext context) throws IOException {
    	key = context.createMapOutputKeyRecord();
    	value = context.createMapOutputValueRecord();
    	value.set(new Object[] {1L });
        System.out.println("TaskID:" + context.getTaskID().toString());
    }
    
    

    @Override
    public void map(long recordNum, Record record, TaskContext context)
        throws IOException {
    	
		key.set(new Object[] { record.get(0).toString(),record.get(1).toString()});
        context.write(key, value);  
    	
    }
  }

  
  public static class SumCombiner extends ReducerBase {
	    private Record count;

	    @Override
	    public void setup(TaskContext context) throws IOException {
	      count = context.createMapOutputValueRecord();
	    }

	    @Override
	    public void reduce(Record key, Iterator<Record> values, TaskContext context)
	        throws IOException {
	    	Long num = 0L;
	        while (values.hasNext()) {
	      	  Record val = values.next();
	      	  num++;
	       }
	        count.set(0, num);
	        context.write(key, count);
	    }
	  }

  
  
  
  /**
   * A reducer class that just emits the sum of the input values.
   **/
  public static class SumReducer extends ReducerBase {
    private Record result = null;
    @Override
    public void setup(TaskContext context) throws IOException {
      result = context.createOutputRecord();
    }

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context)
        throws IOException {
     
      Long num = 0L;
      while (values.hasNext()) {
    	  Record val = values.next();
    	  num += val.getBigint(0);
     }
      result.set(0,key.getString(0));
      result.set(1,key.getString(1));
      result.set(2,num);
	  context.write(result);
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: WordCount <in_table> <out_table>");
      System.exit(2);
    }

    JobConf job = new JobConf();

    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(SumReducer.class);

    job.setMapOutputKeySchema(SchemaUtils.fromString("song_id1:string,song_id2:string"));
    job.setMapOutputValueSchema(SchemaUtils.fromString("count:bigint"));

    InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);

    JobClient.runJob(job);
  }
  }
}












