package hadoop.TianChiMapreduce.GetUserFeature;

import java.awt.List;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

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

public class GetSongEverydayUserNum {

  public static class TokenizerMapper extends MapperBase {
    private Record key;
    private Record value;
    Map<String,Long> dateMap = new HashMap<String,Long>();
    
    private void getDateMap() {
    	Long day = 0L;
    	int date;
    	int[] month_start_day = {20150301,20150401,20150501,20150601,20150701,20150801};
    	int[] month_end_day = {20150331,20150430,20150531,20150630,20150731,20150830};
    	for(int i=0; i<6; i++) {
			for(date=month_start_day[i]; date<=month_end_day[i]; date++ ){
				dateMap.put(Integer.toString(date), day);
				day++;
			}
    	}
    	
    }

    
    @Override
    public void setup(TaskContext context) throws IOException {
    	key = context.createMapOutputKeyRecord();
    	value = context.createMapOutputValueRecord();
        getDateMap();
        System.out.println("TaskID:" + context.getTaskID().toString());
    }
    
    

    @Override
    public void map(long recordNum, Record record, TaskContext context)
        throws IOException {
    	if(record.get(3).toString().endsWith("1")) {
			key.set(new Object[] { record.get(1).toString(),dateMap.get(record.get(4).toString())});
			value.set(new Object[] { record.get(0).toString()});
	        context.write(key, value);  
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
     
     Set<String> user_id = new HashSet<String>();
      while (values.hasNext()) {
    	  Record val = values.next();
    	  user_id.add(val.getString(0));
     }
   
      result.set(0, key.getString(0));
      result.set(1, key.getBigint(1));
      result.set(2, user_id.size());
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

    job.setMapOutputKeySchema(SchemaUtils.fromString("song_id:string,day:bigint"));
    job.setMapOutputValueSchema(SchemaUtils.fromString("user_count:bigint"));

    InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);

    JobClient.runJob(job);
  }
  }
}











