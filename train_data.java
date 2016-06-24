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

public class train_data {

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
    	Object temp_k[] = new Object[9];
    	temp_k[0] = Long.parseLong(record.get(1).toString());
    	for (int i = 2; i < record.getColumnCount(); i++) {
    		temp_k[i-1] = Double.parseDouble(record.get(i).toString());
    	}
    	day_count.set(temp_k);
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
    	//parse
    	int ignore = 50;
    	int before_day = 4;
    	//parse end
    	Double sum = 0.0; // delete mean of everyday less ignore
    	Double[][] map_day = new Double[183][8];
        while (values.hasNext()) {
          Record val = values.next();
          sum += (Double) val.get(1);
          for(int i=0; i<8; i++){
        	  map_day[Integer.parseInt(val.get(0).toString())][i] = (Double)val.get(1+i);
          }
        }
        /*
        if(sum/183.0 < ignore){
        	for(int i=0; i<60; i++){
        		result.set(0, key.get(0));
        		result.set(1, i);
        		result.set(2, (long) (sum/183.0));
        		context.write(result); 
        	}
        }
        */
        /* 0->1, 1->2, 2->3, 3->5, 4->7, 5->10, 6->15, 7->20 */
        if(sum/183.0 >= ignore){
        	for(int i=40; i<181; i++){
        		int k=0;
        		result.set(k, map_day[i+2][2]);//label
        		double mines = 0.0;
        		for(int j=1; j<7; j++)
        			mines += (map_day[i-3*j][2]-map_day[i-3*j-3][2]);
        		k++;
        		result.set(k, mines);//mines
        		k++;
        		result.set(k, map_day[i-3][2]+mines/6.0);//plus_mines
        		k++;
        		result.set(k, map_day[i-1][2]-map_day[i-2][2]);//between_day
        		for(int j=1; j<8; j++){
        			k++;
        			result.set(k, map_day[i-1][j]);//1~7->avg_day
        		}
        		if(map_day[i-1][7] < 1)
        			continue;
        		double xnext1 = map_day[i-7][0];
        		double mines_7day = 0.0;
        		for(int j=0; j<before_day; j++){
        			double xnext = map_day[i-7*(j+2)][0];
        			k++;
        			result.set(k, xnext1-xnext);//before*mines7
        			k++;
        			result.set(k, xnext1);//before*7
        			mines_7day += (xnext1-xnext);
        		}
        		mines_7day /= (1.0*before_day);
        		k++;
        		result.set(k, mines_7day);//mines_7day
        		k++;
        		result.set(k, (long) i%7);//week
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

    job.setMapOutputKeySchema(SchemaUtils.fromString("song:string"));
    job.setMapOutputValueSchema(SchemaUtils.fromString("day:bigint"));
    for(int i=0; i<8; i++)
    	job.setMapOutputValueSchema(SchemaUtils.fromString("count" + i + ":double"));

    InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);

    JobClient.runJob(job);
  }

}
