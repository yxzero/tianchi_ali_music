package hadoop.TianChiMapreduce.evaluationResults;

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

public class GetEveryArtistScore {

  public static class TokenizerMapper extends MapperBase {
    private Record key;
    private Record value;
    private Long tag;
    Map<String,String> dateMap = new HashMap<String,String>();
    
    private void getDateMap() {
    	int day = 0;
    	int date;
    	int[] month_start_day = {20150301,20150401,20150501,20150601,20150701,20150801};
    	int[] month_end_day = {20150331,20150430,20150531,20150630,20150731,20150830};
    	for(int i=0; i<6; i++) {
			for(date=month_start_day[i]; date<=month_end_day[i]; date++ ){
				dateMap.put(Integer.toString(date), Integer.toString(day));
				day++;
			}
    	}
    	
    }

    
    @Override
    public void setup(TaskContext context) throws IOException {
    	key = context.createMapOutputKeyRecord();
    	value = context.createMapOutputValueRecord();
        tag = context.getInputTableInfo().getTableName().equals("song_artist_play_7_8") ? 0L : 1L;
        getDateMap();
        System.out.println("TaskID:" + context.getTaskID().toString());
    }
    
    

    @Override
    public void map(long recordNum, Record record, TaskContext context)
        throws IOException {	 
		key.set(new Object[] { record.get(0).toString()});
		value.set(new Object[] { Long.parseLong(record.get(1).toString()),dateMap.get(record.get(2).toString()),tag});
        context.write(key, value);    
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
      HashMap<String,Long> true_day_map= new HashMap<String,Long>();
      HashMap<String,Long> predict_day_map= new HashMap<String,Long>();
      String day;
      while (values.hasNext()) {
        Record val = values.next();
        day = val.get(1).toString();
        if(val.get(2).toString().equals("0")) {
        	true_day_map.put(day, Long.parseLong(val.get(0).toString()));
        }
        else {
        	predict_day_map.put(day, Long.parseLong(val.get(0).toString()));
        }
     }
      double a = 0;
      long b = 0;
      int num = 0;
      long true_plays,predict_plays;
      for (Map.Entry<String,Long> entry : true_day_map.entrySet()) {  
    	  true_plays = true_day_map.get(entry.getKey());
    	  predict_plays = predict_day_map.get(entry.getKey());
    	  a += Math.pow((predict_plays - true_plays) / (double)true_plays,2.0);
    	  b += true_plays;
    	  num++;  
      }  
      double a1 = Math.sqrt(a/num);
      double b1 = Math.sqrt((double)b);
      result.set(0, key.get(0).toString());
      result.set(1, a1);
      result.set(2, b1);
      result.set(3, (1.0-a1)*b1);
      context.write(result);  
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: WordCount <in_table> <out_table>");
      System.exit(2);
    }

    JobConf job = new JobConf();

    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(SumReducer.class);

    job.setMapOutputKeySchema(SchemaUtils.fromString("artist_id:string"));
    job.setMapOutputValueSchema(SchemaUtils.fromString("count:bigint,day:string,tag:bigint"));

    InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
    InputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName(args[2]).build(), job);

    JobClient.runJob(job);
  }
  }
}










