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

public class test_data_1 {

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
    	Object temp_k[] = new Object[13];
    	temp_k[0] = Long.parseLong(record.get(1).toString());
    	for (int i = 2; i < record.getColumnCount(); i++) {
    		if(i == 10)
    			temp_k[i-1] = record.get(i).toString();
    		else
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
    	int ignore = 30;
    	int before_day = 4;
    	//parse end
    	Double sum = 0.0; // delete mean of everyday less ignore
    	Double sum_20 = 0.0;
    	Double[][] map_day = new Double[183][8];
    	Record val = null;
        while (values.hasNext()) {
          val = values.next();
          sum += (Double) val.get(1);
          if(Integer.parseInt(val.get(0).toString()) >= 163)
        	  sum_20 += (Double) val.get(1);
          for(int i=0; i<8; i++){
        	  map_day[Integer.parseInt(val.get(0).toString())][i] = (Double)val.get(1+i);
          }
        }
        //get publish_time
        Double publish_time = 0.0;
        SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd");
        try {
			Date dt = sdf.parse(val.get(9).toString());
			Date bdt = sdf.parse("20150301");
			publish_time = (double) ((bdt.getTime()-dt.getTime())/(24*60*60*1000));
        } catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
        }
        Double song_init_plays = (Double)val.get(10);
        Double language = (Double)val.get(11);
        Double gender = (Double)val.get(12);
        //get publish_time end
        
        if(sum/183.0 < ignore || is_stable(182, map_day)){
        	for(int i=0; i<60; i++){
        		result.set(0, key.get(0));
        		result.set(1, i);
        		result.set(2, (long) (sum_20/20.0));
        		context.write(result); 
        	}
        }
        if(publish_time < (20-183)){
        	double k_p1 = map_day[182][0];
        	double k_p2 = map_day[181][0];
        	double tempkp = 0.0;
        	for(int i=0; i<60; i++){
        		result.set(0, key.get(0));
        		result.set(1, i);
        		tempkp = (k_p1+k_p2)/2.0;
        		result.set(2, (long) (tempkp));
        		k_p2 = k_p1;
        		k_p1 = tempkp;
        		context.write(result); 
        	}
        }
        
        /* 0->1, 1->2, 2->3, 3->5, 4->7, 5->10, 6->15, 7->20 */
        /*
        if(sum/183.0 >= ignore){
        	for(int i=183; i<184; i++){
        	//for(int i=121; i<122; i++){
        		if(is_stable(182, map_day) || publish_time < (20-183))
        			continue;
        		int k=0;
        		result.set(k, key.get(0));//song_id
        		k++;
        		result.set(k, i);
        		double mines = 0.0;
        		for(int j=1; j<7; j++){
        			k++;
        			result.set(k, map_day[i-3*j][2]);//avg_3
        			mines += (map_day[i-3*j][2]-map_day[i-3*j-3][2]);
        		}
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
        		k++;
        		result.set(k, publish_time);//publish_time
        		k++;
        		result.set(k, song_init_plays);//song_init_plays
        		k++;
        		result.set(k, language);//language
        		k++;
        		result.set(k, gender);//gender
        		k++;
        		if(publish_time<1)//avg_init_play
        			result.set(k, 0.0);
        		else
        			result.set(k, song_init_plays/publish_time);
        		context.write(result); 
        	}
        }
        */
    }
	private boolean is_stable(int i, Double[][] map_day) {
		// TODO Auto-generated method stub
		double avg_20 = 0.0;
		double avg_sqr = 0.0;
		double max_day = map_day[i][0];
		double min_day = map_day[i][0];
		for(int j=0; j<20; j++){
			max_day = Math.max(max_day, map_day[i-j][0]);
			min_day = Math.min(min_day, map_day[i-j][0]);
			avg_20 += map_day[i-j][0];
			avg_sqr += Math.pow(map_day[i-j][0], 2);
		}
		avg_20 /= 20.0;
		avg_sqr /= 20.0;
		double sd = Math.sqrt(avg_sqr - Math.pow(avg_20, 2));
		if(max_day-min_day<40 || sd<(avg_20*0.15) || sd<30)
			return true;
		return false;
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
    job.setMapOutputValueSchema(SchemaUtils.fromString("count8:string"));
    for(int i=9; i<12; i++){
    	job.setMapOutputValueSchema(SchemaUtils.fromString("count" + i + ":double"));
    }

    InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);

    JobClient.runJob(job);
  }

}
