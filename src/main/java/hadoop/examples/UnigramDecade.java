package hadoop.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;




public class UnigramDecade {
 
    public static class MapperClass extends Mapper<LongWritable,Text,Text ,LongWritable> {
        private final Text outKey = new Text();
        private final LongWritable outValue = new LongWritable();

	
    @Override
    public void setup(Context context)  throws IOException, InterruptedException {
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {

        String line = value.toString();
        String[] parts = line.split("\\s+");
        if (parts.length < 3)
            return;
        String word;
        int year;
        long count;
        try {
            word = parts[0];
            year = Integer.parseInt(parts[1]);
            count = Long.parseLong(parts[2]);
        }catch (NumberFormatException e){return;}
        int decade =(year/10) * 10;
        String decade_str = decade + "";
        outKey.set(decade_str + "\t" + word);
        outValue.set(count);
        context.write(outKey, outValue);
        outKey.set(decade_str + "\t*TOTAL*");
        context.write(outKey, outValue);
    }
    
    @Override
    public void cleanup(Context context)  throws IOException, InterruptedException {
    }

  }
 
  public static class ReducerClass extends Reducer<Text,LongWritable,Text,LongWritable> {
      private final LongWritable outValue = new LongWritable();

	    @Override
	    public void setup(Context context)  throws IOException, InterruptedException {
	    }

	    @Override
	    public void reduce(Text key, Iterable<LongWritable> counts, Context context) throws IOException,  InterruptedException {
            long sum = 0;
            for (LongWritable count : counts) {
                sum+=count.get();
            }
            outValue.set(sum);
            context.write(key,outValue);
	    }
	    
	    @Override
	    public void cleanup(Context context)  throws IOException, InterruptedException {
	    }
	 }
 
  public static class CombinerClass 
     extends Reducer<Text,LongWritable,Text,LongWritable> {
      private final LongWritable outValue = new LongWritable();

	    @Override
	    public void setup(Context context)  throws IOException, InterruptedException {
	    }

	    @Override
	    public void reduce(Text key, Iterable<LongWritable> counts, Context context) throws IOException,  InterruptedException {
            long sum = 0;
            for (LongWritable count : counts) {
                sum += count.get();
            }
            outValue.set(sum);
            context.write(key,outValue);
	    }
	    
	    @Override
	    public void cleanup(Context context)  throws IOException, InterruptedException {
	    }
	 }

  public static class PartitionerClass extends Partitioner<Text,IntWritable> {
	  
      @Override
      public int getPartition(Text key, IntWritable value, int numReducers) {
        return key.hashCode() % numReducers;
      }
    
    }
 
  
 public static void main(String[] args) throws Exception {
     if (args.length != 2) {
         System.err.println("invalid input");
         System.exit(1);
     }
     Configuration conf = new Configuration();
     Job job = Job.getInstance(conf, "UnigramDecade");
     job.setJarByClass(UnigramDecade.class);

     job.setMapperClass(MapperClass.class);
     job.setCombinerClass(ReducerClass.class);
     job.setReducerClass(ReducerClass.class);

     job.setMapOutputKeyClass(Text.class);
     job.setMapOutputValueClass(LongWritable.class);

     job.setOutputKeyClass(Text.class);
     job.setOutputValueClass(LongWritable.class);

     job.setInputFormatClass(TextInputFormat.class);
     job.setOutputFormatClass(TextOutputFormat.class);

     FileInputFormat.addInputPath(job, new Path(args[0]));
     FileOutputFormat.setOutputPath(job, new Path(args[1]));

     System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
 
}
