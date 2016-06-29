package yx_song_day.testxxx;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
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

public class artist2play {

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
    	song_id.set(new Object[] { record.get(0).toString()+","+record.get(2).toString() });
    	day_count.set(new Object[] { Double.parseDouble(record.get(3).toString()) });
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
    	double count = 0.0;
    	while (values.hasNext()) {
            Record val = values.next();
            count += val.getDouble(0);
          }
    	String[] a = key.get(0).toString().split(",");
    	int day = Integer.parseInt(a[1]);
    	SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd");
    	Calendar cal = Calendar.getInstance();
    	Date bdt;
    	try {
    		bdt = sdf.parse("20150901");
    		cal.setTime(bdt);
    	} catch (ParseException e1) {
    		// TODO Auto-generated catch block
    		e1.printStackTrace();
    	}
    	result.set(0, a[0]);
    	result.set(1, ""+(int)count);
    	System.out.println(count);
    	cal.add(Calendar.DATE, day);
        result.set(2, (new SimpleDateFormat("yyyyMMdd")).format(cal.getTime()));
        context.write(result); 
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
    job.setMapOutputValueSchema(SchemaUtils.fromString("count:double"));

    InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);

    JobClient.runJob(job);
  }

}
