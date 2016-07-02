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

public class combine2data {

  public static class TokenizerMapper extends MapperBase {
    private Record song_id;
    private Record day_count;
    private long tag;

    @Override
    public void setup(TaskContext context) throws IOException {
      song_id = context.createMapOutputKeyRecord();
      day_count = context.createMapOutputValueRecord();
      tag = context.getInputTableInfo().getTableName().equals("song_day_use_test") ? 0 : 1;
      System.out.println("TaskID:" + context.getTaskID().toString());
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context)
        throws IOException {
    	Long day = Long.valueOf(record.get(1).toString());
    	if(tag==0)
    		day -= 183;
    	song_id.set(new Object[] { record.get(0).toString() });
    	day_count.set(new Object[] { day ,  Double.parseDouble(record.get(2).toString()) });
    	context.write(song_id, day_count);
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
    	Double[] map_day = new Double[60];
    	result.set(0, key.get(0));
    	Long maxday = (long) 0;
    	while (values.hasNext()) {
        	Record val = values.next();
        	Long day = val.getBigint(0);
        	result.set(1, day);
        	result.set(2, val.get(1));
        	context.write(result); 
        	map_day[day.intValue()] = (Double) val.get(1);
        	if(day > maxday)
        		maxday = day;
    	}
        if(maxday<59){
        	for(int i=maxday.intValue()+1; i<60; i++){
        		double temp = 0.0;
        		for(int i_j=1; i_j<=20 && (i-i_j)>=0; i_j++)
        			temp += map_day[i-i_j];
        		map_day[i] = temp/20.0;
        		result.set(1, (long) i);
            	result.set(2, temp/20.0);
            	context.write(result); 
        	}
        }
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: WordCount <in_table> <out_table>");
      System.exit(2);
    }

    JobConf job = new JobConf();

    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(SumReducer.class);

    job.setMapOutputKeySchema(SchemaUtils.fromString("song:string"));
    job.setMapOutputValueSchema(SchemaUtils.fromString("day:bigint"));
    job.setMapOutputValueSchema(SchemaUtils.fromString("count:double"));

    InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
    InputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName(args[2]).build(), job);

    JobClient.runJob(job);
  }

}
