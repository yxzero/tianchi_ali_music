package hadoop.TianChiMapreduce;
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
import com.google.common.collect.Multiset.Entry;

public class GetPeakModelResult {

  public static class TokenizerMapper extends MapperBase {
    private Record key;
    private Record value;
    private int predict_day = 11;
    private Integer true_day;
    private Double mean_20_play;
    
    @Override
    public void setup(TaskContext context) throws IOException {
    	key = context.createMapOutputKeyRecord();
    	value = context.createMapOutputValueRecord();
        System.out.println("TaskID:" + context.getTaskID().toString());
    }
    
    

    @Override
    public void map(long recordNum, Record record, TaskContext context)
        throws IOException {
    	true_day = Integer.parseInt(record.get(2).toString())-3;
    	mean_20_play = Double.parseDouble(record.get(82).toString());
    	
    	key.set(new Object[] { record.get(0).toString()});
    	value.set(new Object[] {-1L,mean_20_play});
        context.write(key, value);
        
    	if(true_day >= predict_day-1){
    		return;
    	}
    	
		Long day = 0L;
		int row_index = true_day + 7;
		for(int i=true_day+2; i <= predict_day; i++) {
			value.set(new Object[] {day ,Double.parseDouble(record.get(row_index).toString())});
	        context.write(key, value);
	        day++;
	        row_index++;
		}
    	
    }
  }

  /**
   * A reducer class that just emits the sum of the input values.
   **/
  public static class SumReducer extends ReducerBase {
    private Record result = null;
    private Double mean_20_play;
    @Override
    public void setup(TaskContext context) throws IOException {
      result = context.createOutputRecord();
    }

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context)
        throws IOException {
     double[] day_play = new double[60];
     Map<Long,Double> day_map = new HashMap<Long,Double>();
      long day;
      while (values.hasNext()) {
    	  Record val = values.next();
    	  day = val.getBigint(0);
    	  if(day == -1) {
    		  mean_20_play = val.getDouble(1);
    	  }
    	  else {
    		  day_map.put(new Long(val.getBigint(0)), new Double(val.getDouble(1)));
    	  }
     }
     
     for(int i=0; i<60; i++) {
    	 day_play[i] = mean_20_play;
     }
      
      for (Map.Entry<Long,Double> entry : day_map.entrySet()) {  
    	  day_play[entry.getKey().intValue()] = entry.getValue(); 
      }  
      
      result.set(0, key.getString(0));
      for(Long i=0L; i<60; i++) {
    	  result.set(1, i);
    	  result.set(2, day_play[i.intValue()]);
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

    job.setMapOutputKeySchema(SchemaUtils.fromString("song_id:string"));
    job.setMapOutputValueSchema(SchemaUtils.fromString("day:bigint,count:double"));

    InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);

    JobClient.runJob(job);
  }
  
}













