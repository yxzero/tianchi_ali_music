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

public class avg_day {

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
    	song_id.set(new Object[] { record.get(0).toString() });
    	day_count.set(new Object[] { Long.valueOf(record.get(1).toString()), Double.parseDouble(record.get(2).toString()) });
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
    	HashMap<Long , Double> map = new HashMap<Long , Double>();
        while (values.hasNext()) {
        	Record val = values.next();
        	map.put((Long)val.get(0), (Double) val.get(1));
        }
        Iterator<Long> it = map.keySet().iterator();
        int a[] = {1,2,3,5,7,10,15,20};
        while(it.hasNext()) {  
            Long day_i = (Long) it.next();
            result.set(0, key.get(0));
            result.set(1, day_i);
            int k = 0;
            int kt = 0;
            double sum = 0.0;
            for(int i=0; i<8; i++){
            	while( k != a[i]){
            		k++;
            		if(map.containsKey(day_i-k)){
            			sum += map.get(day_i-k);
            			kt++;
            		}
            	}
                result.set(i+2, sum/( ((double) kt) * 1.0 ));
            }
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

    job.setMapOutputKeySchema(SchemaUtils.fromString("song:string"));
    job.setMapOutputValueSchema(SchemaUtils.fromString("day:bigint"));
    job.setMapOutputValueSchema(SchemaUtils.fromString("count:double"));

    InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);

    JobClient.runJob(job);
  }

}
