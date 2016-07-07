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

public class GetTwoSongCommonUserSplit {

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
    	
		key.set(new Object[] { record.get(0).toString()});
		value.set(new Object[] { record.get(1).toString()});
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
     
      Set<String> song_id_list = new HashSet<String>();
      
      while (values.hasNext()) {
    	  Record val = values.next();
    	  song_id_list.add(new String(val.getString(0))); 
     }
     
     String[] song_id_Array = new String[song_id_list.size()];
     
     int index=0;
     for(String song_id:song_id_list) {
    	 song_id_Array[index++] = song_id;
     }
     
     int j;
     for(int i=0; i<index; i++) {
    	 for(j=i+1; j<index; j++) {
    		 result.set(0, song_id_Array[i]);
    		 result.set(1, song_id_Array[j]);
    		 context.write(result);  
    		 
    		 result.set(0, song_id_Array[j]);
    		 result.set(1, song_id_Array[i]);
    		 context.write(result);  
    	 }
     }
     
     
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: WordCount <in_table> <out_table>");
      System.exit(2);
    }

    JobConf job = new JobConf();

    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(SumReducer.class);

    job.setMapOutputKeySchema(SchemaUtils.fromString("user_id:string"));
    job.setMapOutputValueSchema(SchemaUtils.fromString("song_id:string"));

    InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);

    JobClient.runJob(job);
  }
  }
}












