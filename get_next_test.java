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

public class get_next_test {

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
      tag = context.getInputTableInfo().getTableName().equals("song_day") ? 0 : 1;
      System.out.println("TaskID:" + context.getTaskID().toString());
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context)
        throws IOException {
    	song_id.set(0, record.get(0).toString());
		song_id.set(1, tag);
		//System.out.println(temp_table_name);
    	if(tag==0){
    		Object temp_k[] = new Object[9];
    		temp_k[0] = Long.parseLong(record.get(1).toString());
    		for (int i = 2; i < record.getColumnCount(); i++) {
    			temp_k[i-1] = Double.parseDouble(record.get(i).toString());
    		}
    		day_count.set(temp_k);
    		context.write(song_id, day_count);
    	}
    	else{
    		Object temp_k[] = new Object[9];
    		temp_k[0] = Long.parseLong(record.get(1).toString());
    		temp_k[1] = Double.parseDouble(record.get(2).toString());
    		for(int i=2; i<9; i++)
    			temp_k[i] = 0.0;
    		day_count.set(temp_k);
    		context.write(song_id, day_count);
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
    	HashMap<Long , Double> map = new HashMap<Long , Double>();
    	Double[][] map_day = new Double[244][8];
    	//parse
    	int ignore = 50;
    	int before_day = 4;
    	//parse end
    	Double sum = 0.0; // delete mean of everyday less ignore
    	Long maxday = (long) 0; // now contain day
        while (values.hasNext()) {
        	Record val = values.next();
        	long tag = (Long) key.get(1);
        	//System.out.println("reduce:"+key.get(0).toString()+":"+tag);
        	if(tag==1){
        		Long days = (Long)val.get(0);
        		map.put(days, (Double)val.get(1));
        		if(days > maxday)
        			maxday = days;
        	}
        	else{
        		sum += (Double) val.get(1);
                for(int i=0; i<8; i++){
              	  	map_day[Integer.parseInt(val.get(0).toString())][i] = (Double)val.get(1+i);
                }
        	}
        }
        process_add_day(map, map_day, maxday);
        //System.out.println(""+sum/183+" maxday:"+maxday);
        /* 0->1, 1->2, 2->3, 3->5, 4->7, 5->10, 6->15, 7->20 */
        if(sum/183.0 >= ignore){
        	int i = (int) (maxday+1);
        	int k=0;
        	result.set(k, key.get(0));//song_id
        	k++;
        	result.set(k, i);
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

	private void process_add_day(HashMap<Long, Double> map, Double[][] map_day,
			Long maxday) {
		// TODO Auto-generated method stub
		int a[]={1,2,3,5,7,10,15,20};
        for(Long i=(long) 183; i<=maxday; i++){
        	int k=0;
            Double sum=0.0;
            for(int j=0; j<8; j++){
            	while(k!=a[j]){
            		if(map.containsKey(i-k)){
            			sum += (Double) map.get(i-k);
            		}
            		else
            			sum += map_day[(int) (i-k)][0];
            		k++;
            	}
            	if(k > 0)
            		map_day[i.intValue()][j] = sum/(( (double) k ) * 1.0);
            	else
            		map_day[i.intValue()][j] = sum;
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

    job.setMapOutputKeySchema(SchemaUtils.fromString("key:string,tag:bigint"));
    job.setMapOutputValueSchema(SchemaUtils.fromString("day:bigint"));
    for(int i=0; i<8; i++)
    	job.setMapOutputValueSchema(SchemaUtils.fromString("count" + i + ":double"));
    
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
