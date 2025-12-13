package hadoop.examples;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

public class BigramDecade {
    public static class MapperClass extends Mapper<LongWritable,Text,Text ,LongWritable> {
        private final Text outKey = new Text();
        private final LongWritable outValue = new LongWritable();
        Set<String> stopWords;

        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
            stopWords = new HashSet<String>();
            Configuration conf = context.getConfiguration();
            String engPath = conf.get("stopwords.en.path");
            String hebPath = conf.get("stopwords.heb.path");
            loadStopWords(engPath,stopWords);
            loadStopWords(hebPath,stopWords);
        }
        private void loadStopWords(String path, Set<String> set) throws IOException {
            if (path == null || path.isEmpty()) return;
            try(BufferedReader br = new BufferedReader(new FileReader(path))){
                String line;
                while ((line = br.readLine()) != null){
                    line = line.trim();
                    if (!line.isEmpty())
                        set.add(line);
                }
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\t");
            if (parts.length <3)
                return;
            String[] words;
            int year;
            long count;
            try{
                words = parts[0].split(" ");
                year = Integer.parseInt(parts[1]);
                count = Long.parseLong(parts[2]);
            }catch (NumberFormatException  e){return;}
            int decade = (year/10) *10;
            String decade_str = decade + "";
            if (stopWords.contains(words[0]) || stopWords.contains(words[1])){
                return;
            }
            outKey.set(decade_str+ "\t" + words[0] + "\t" + words[1]);
            outValue.set(count);
            context.write(outKey,outValue);
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
            for (LongWritable count : counts){
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
            long sum =0;
            for (LongWritable count : counts){
                sum+=count.get();
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
        if (args.length != 4) {
            System.err.println("invalid input");
            System.exit(1);
        }
        Configuration conf = new Configuration();

        conf.set("stopwords.en.path", args[2]);
        conf.set("stopwords.heb.path", args[3]);

        Job job = Job.getInstance(conf, "BigramDecade");
        job.setJarByClass(BigramDecade.class);

        job.setMapperClass(BigramDecade.MapperClass.class);
        job.setCombinerClass(BigramDecade.ReducerClass.class);
        job.setReducerClass(BigramDecade.ReducerClass.class);

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
