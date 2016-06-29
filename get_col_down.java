package yx_song_day.testxxx;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

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

public class get_col_down {

  public static class TokenizerMapper extends MapperBase {
    private Record song_id;
    private Record day_count;
    private long tag;
    private String temp_label;
    private String temp_table_name;

    @Override
    public void setup(TaskContext context) throws IOException {
      song_id = context.createMapOutputKeyRecord();
      day_count = context.createMapOutputValueRecord();
      tag = context.getInputTableInfo().getTableName().equals("song_collect_mean") ? 0 : 1;
      System.out.println("TaskID:" + context.getTaskID().toString());
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context)
        throws IOException {
    	song_id.set(0, record.get(0).toString());
		song_id.set(1, tag);
		//System.out.println(temp_table_name);
		day_count.set(new Object[] { Long.parseLong(record.get(1).toString()), Double.parseDouble(record.get(3).toString()) });
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
    	Double[] map_col = new Double[183];
    	Double[] map_download = new Double[183];
    	while (values.hasNext()) {
              Record val = values.next();
              long tag = (Long) key.get(1);
              if(tag == 0)
            	  map_col[Integer.parseInt(val.get(0).toString())] = (Double)val.get(1);
              else
            	  map_download[Integer.parseInt(val.get(0).toString())] = (Double)val.get(1);
        }
    	result.set(0, key.get(0).toString());
    	int k = 2;
    	for(int i=0; i<183; i+=10){
    		result.set(k, map_col[i]);
    		k++;
    	}
    	for(int i=0; i<183; i+=10){
    		result.set(k, map_download[i]);
    		k++;
    	}
    	for(long i=0; i<183; i++){
    		result.set(1, i);
    		context.write(result);
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

    job.setMapOutputKeySchema(SchemaUtils.fromString("key:string,tag:bigint"));
    job.setMapOutputValueSchema(SchemaUtils.fromString("day:bigint"));
    job.setMapOutputValueSchema(SchemaUtils.fromString("count:double"));
    
    job.setPartitionColumns(new String[]{"key"});
    job.setOutputKeySortColumns(new String[]{"key", "tag"});
    job.setOutputGroupingColumns(new String[]{"key"});
    job.setNumReduceTasks(1);
    InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
    InputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName(args[2]).build(), job);

    JobClient.runJob(job);
  }

}
