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

public class GetSongNewOldUserPlay {

  public static class TokenizerMapper extends MapperBase {
    private Record key;
    private Record value;
    
    @Override
    public void setup(TaskContext context) throws IOException {
    	key = context.createMapOutputKeyRecord();
    	value = context.createMapOutputValueRecord();
        System.out.println("TaskID:" + context.getTaskID().toString());
    }
    
    

    @Override
    public void map(long recordNum, Record record, TaskContext context)
        throws IOException {
    	String[] day_user_play = record.get(2).toString().split(",");
    	Long day = 0L;
    	String[] user_play;
    	for(String item:day_user_play){
    		if(!item.equals("0")){
    			user_play = item.split("-");
    			key.set(new Object[] {record.get(1).toString(),day});
    			value.set(new Object[] {user_play[1],Long.parseLong(user_play[0])});
    			context.write(key, value); 
    		}
    		day++;
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
     
      Long new_user_num = 0L;
      Long old_user_num = 0L;
      Long new_play_num = 0L;
      Long old_play_num = 0L;
      while (values.hasNext()) {
    	  Record val = values.next();
    	  if(val.get(0).toString().equals("new")) {
    		  new_user_num += 1;
    		  new_play_num += val.getBigint(1);
    	  }
    	  else {
    		  old_user_num += 1;
    		  old_play_num += val.getBigint(1);
    	  }
     }
     double new_all_user_rate = (double)new_user_num / (new_user_num + old_user_num);
     double new_all_play_rate = (double)new_play_num / (new_play_num + old_play_num);
      
      
      result.set(0, key.get(0).toString());
      result.set(1, key.getBigint(1));
      result.set(2, new_user_num);
      result.set(3, old_user_num);
      result.set(4, new_play_num);
      result.set(5, old_play_num);
      result.set(6, new_all_user_rate);
      result.set(7, new_all_play_rate);
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
    job.setMapOutputValueSchema(SchemaUtils.fromString("new_old_user:string,new_old_play:bigint"));

    InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);

    JobClient.runJob(job);
  }
  }
}











