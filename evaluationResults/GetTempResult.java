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

public class GetTempResult {

  public static class TokenizerMapper extends MapperBase {
    private Record key;
    private Record value;
    private long tag;

    @Override
    public void setup(TaskContext context) throws IOException {
      key = context.createMapOutputKeyRecord();
      value = context.createMapOutputValueRecord();
      tag = context.getInputTableInfo().getTableName().equals("song_play_7_8") ? 0 : 1;
      System.out.println("TaskID:" + context.getTaskID().toString());
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context)
        throws IOException {
    key.set(new Object[] { record.get(0).toString()});
    if(tag == 0) {
    	value.set(new Object[] { record.get(1).toString(),record.get(2).toString(),""});
    }
    else {
    	value.set(new Object[] { "","",record.get(1).toString()});
    }
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
    	Iterator<Record> items = values;
        String artist_id = "";
        while (values.hasNext()) {
          Record val = values.next();
          if(val.get(0).toString().equals("")) {
          	artist_id = val.get(2).toString();
            break;
       }
       while (items.hasNext()){
    	   Record vall = values.next();
    	   if(!vall.get(0).toString().equals("")){
    		   result.set(0, key.get(0).toString());
               result.set(1, vall.get(0).toString());
               result.set(2, vall.get(1).toString());
               result.set(3, artist_id);
               context.write(result);
    	   }   
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
    job.setMapOutputValueSchema(SchemaUtils.fromString("ds:string"));
    job.setMapOutputValueSchema(SchemaUtils.fromString("count:string"));
    job.setMapOutputValueSchema(SchemaUtils.fromString("artist_id:string"));


    InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);

    JobClient.runJob(job);
  }
  }
}


