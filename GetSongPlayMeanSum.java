package hadoop.TianChiMapreduce;

import java.awt.List;
import java.io.IOException;
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

public class GetSongPlayMeanSum {
  public static int mean_days = 20;

  public static class TokenizerMapper extends MapperBase {
    private Record word;
    private Record one;
    private int due_date = 182 - mean_days;
    
    Map<String,Integer> dateMap = new HashMap<String,Integer>();
    Map<String,String> dayMap = new HashMap<String,String>();
    
    private void getDateMap() {
    	int day = 0;
    	int date;
    	int[] month_start_day = {20150301,20150401,20150501,20150601,20150701,20150801};
    	int[] month_end_day = {20150331,20150430,20150531,20150630,20150731,20150830};
    	for(int i=0; i<6; i++) {
			for(date=month_start_day[i]; date<=month_end_day[i]; date++ ){
				dateMap.put(Integer.toString(date), day);
				dayMap.put(Integer.toString(day), Integer.toString(date));
				day++;
			}
    	}
    	
    }

    @Override
    public void setup(TaskContext context) throws IOException {
      word = context.createMapOutputKeyRecord();
      one = context.createMapOutputValueRecord();
      one.set(new Object[] { 1L });
      System.out.println("TaskID:" + context.getTaskID().toString());
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context)
        throws IOException {
    getDateMap();
	if (record.get(3).toString().equals("1")) {
		if(dateMap.get(record.get(4).toString()) > due_date) {
			word.set(new Object[] { record.get(1).toString()});
	        context.write(word, one);	
		}
	}
      
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
      long count = 0;
      while (values.hasNext()) {
        Record val = values.next();
        count++;
     }
      result.set(0, key.get(0).toString());
      result.set(1, count);
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

    job.setMapOutputKeySchema(SchemaUtils.fromString("word:string"));
    job.setMapOutputValueSchema(SchemaUtils.fromString("count:bigint"));

    InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);

    JobClient.runJob(job);
  }
  }
}

