package hadoop.TianChiMapreduce;

import java.awt.List;
import java.io.IOException;
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

class GetSongDownloadMeanSomedays {

	public static class TokenizerMapper extends MapperBase {
	    private Record song_id;
	    private Record day_count;
	    
	    //****
	    
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
	    	day_count.set(new Object[] { Long.parseLong(record.get(1).toString()), Double.parseDouble(record.get(2).toString()) });
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
	      	long temp;
	          Record val = values.next();
	          map.put((Long)val.get(0), (Double)val.get(1));
	        }
	        int a[]={1,3,20};
	        result.set(0, key.get(0));
	        for(Long i=(long) 0; i<183; i++){
	        	result.set(1, i);
	        	int k=0;
	            Double sum=0.0;
	            int kk=0;
	            for(int j=0; j<3; j++){
	            	while(k!=a[j]){
	            		if(map.containsKey(i-k)){
	            			kk++;
	            			sum += (Double) map.get(i-k);
	            		}
	            		k++;
	            	}
	            	if(kk > 0)
	            		result.set(j+2, sum/(( (double) kk ) * 1.0));
	            	else
	            		result.set(j+2, sum);
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
