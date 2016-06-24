package yx_song_day.testxxx;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

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

public class song1day {

  public static class TokenizerMapper extends MapperBase {
    private Record song_id;
    private Record day_count;

    @Override
    public void setup(TaskContext context) throws IOException {
      song_id = context.createMapOutputKeyRecord();
      day_count = context.createMapOutputValueRecord();
      System.out.println("TaskID:" + context.getTaskID().toString());
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context)
        throws IOException {
    	if(record.get(3).toString().equals("1")){
    		song_id.set(new Object[] { record.get(1).toString() });
    		day_count.set(new Object[] { record.get(4).toString(), 1L });
    		context.write(song_id, day_count);
    	}
    }
  }

  /**
   * A combiner class that combines map output by sum them.
   **/
  public static class SumCombiner extends ReducerBase {
    private Record count;

    @Override
    public void setup(TaskContext context) throws IOException {
      count = context.createMapOutputValueRecord();
    }

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context)
        throws IOException {
      HashMap<String , Long> map = new HashMap<String , Long>();
      while (values.hasNext()) {
    	long temp;
        Record val = values.next();
        if( !map.containsKey(val.get(0).toString()) ){
        	map.put(val.get(0).toString(), (long) 1);
        }
        else{
        	temp = map.get(val.get(0).toString()) + 1;
        	map.put(val.get(0).toString(), temp);
        }
      }
      Iterator<String> it = map.keySet().iterator();  
      while(it.hasNext()) {  
          String day_i = (String)it.next();  
          count.set(0, day_i);
          count.set(1, map.get(day_i));
          context.write(key, count); 
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
    	HashMap<String , Long> map = new HashMap<String , Long>();
        while (values.hasNext()) {
      	long temp;
          Record val = values.next();
          if( !map.containsKey(val.get(0).toString()) ){
          	map.put(val.get(0).toString(), (long) 1);
          }
          else{
          	temp = map.get(val.get(0).toString()) + 1;
          	map.put(val.get(0).toString(), temp);
          }
        }
        SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd");
        long day = 0;
        Iterator<String> it = map.keySet().iterator();  
        while(it.hasNext()) {  
            String day_i = (String)it.next();  
            try {
    			Date dt = sdf.parse(day_i);
    			Date bdt = sdf.parse("20150301");
    			day = (dt.getTime()-bdt.getTime())/(24*60*60*1000);
            } catch (ParseException e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
            }
            result.set(0, key.get(0));
            result.set(1, day);
            result.set(2, (double) map.get(day_i));
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
    job.setCombinerClass(SumCombiner.class);
    job.setReducerClass(SumReducer.class);

    job.setMapOutputKeySchema(SchemaUtils.fromString("song:string"));
    job.setMapOutputValueSchema(SchemaUtils.fromString("day:string"));
    job.setMapOutputValueSchema(SchemaUtils.fromString("count:bigint"));

    InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);

    JobClient.runJob(job);
  }

}
