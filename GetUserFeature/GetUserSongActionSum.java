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

public class GetUserSongActionSum {

  public static class TokenizerMapper extends MapperBase {
    private Record key;
    private Record value;
    Map<String,String> dateMap = new HashMap<String,String>();
    
    private void getDateMap() {
    	Integer day = 0;
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
        getDateMap();
        System.out.println("TaskID:" + context.getTaskID().toString());
    }
    
    

    @Override
    public void map(long recordNum, Record record, TaskContext context)
        throws IOException {
		key.set(new Object[] { record.get(0).toString(),record.get(1).toString()});
		value.set(new Object[] { dateMap.get(record.get(4).toString()),1L,record.get(3).toString()});
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
     int[] day_play = new int[183];
     for(int i=0; i<183; i++) {
    	 day_play[i] = 0;
     }
     
     ArrayList<Integer> download_list = new ArrayList<Integer>();
     ArrayList<Integer> collect_list = new ArrayList<Integer>();
     
     Long play_sum = 0L;
      while (values.hasNext()) {
    	  Record val = values.next();
    	  if(val.get(2).toString().equals("1")) {
	    	  day_play[Integer.parseInt(val.get(0).toString())]++;
	    	  play_sum++;
    	  }
    	  else if(val.get(2).toString().equals("2")) {
    		  download_list.add(Integer.parseInt(val.get(0).toString()));
    	  }
    	  else {
    		  collect_list.add(Integer.parseInt(val.get(0).toString()));
    	  }
     }
     
      StringBuffer everyday_play = new StringBuffer();
      for(int i=0; i<182; i++) {
    	  everyday_play.append(String.valueOf(day_play[i]));
    	  if(download_list.contains(i)) {
    		  everyday_play.append("Download");
    	  }
    	  if(collect_list.contains(i)) {
    		  everyday_play.append("Collect");
    	  }
    	  everyday_play.append(",");
      }
      everyday_play.append(String.valueOf(day_play[182]));
      
      result.set(0, key.get(0).toString());
      result.set(1, key.get(1).toString());
      result.set(2, everyday_play.toString());
      result.set(3, play_sum);
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

    job.setMapOutputKeySchema(SchemaUtils.fromString("user_id:string,song_id:string"));
    job.setMapOutputValueSchema(SchemaUtils.fromString("day:string,count:bigint,action:string"));

    InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);

    JobClient.runJob(job);
  }
  }
}












