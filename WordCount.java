package hadoop.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
  
public class WordCount { 
 
public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    @Override
    public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
      for (String word : line.toString().split("\\s+")) 
        context.write(new Text(word), new IntWritable(1));
    }
  }
 
  public static class ReducerClass extends Reducer<Text,IntWritable,Text,IntWritable> {
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }
      context.write(key, new IntWritable(sum)); 
    }
  }
 
    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
      @Override
      public int getPartition(Text key, IntWritable value, int numPartitions) {
        return key.hashCode() % numPartitions;
      }    
    }
 
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(MapperClass.class);
    job.setPartitionerClass(PartitionerClass.class);
    job.setCombinerClass(ReducerClass.class);
    job.setReducerClass(ReducerClass.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    
  }
 
}