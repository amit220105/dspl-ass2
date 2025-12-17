package hadoop.examples;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class BigramDecade {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final Text outKey = new Text();
        private final LongWritable outValue = new LongWritable();
        Set<String> stopWords;

        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null || cacheFiles.length < 2) {
                throw new IOException("Stopwords cache files missing. Expected 2 files (eng + heb).");
            }

            stopWords = new HashSet<>();
            Configuration conf = context.getConfiguration();

            for (URI uri : cacheFiles) {
                Path p = new Path(uri);

               
                FileSystem fs = p.getFileSystem(conf);

                try (FSDataInputStream in = fs.open(p);
                    BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {

                    String line;
                    while ((line = br.readLine()) != null) {
                        line = line.trim();
                        if (!line.isEmpty() && !line.startsWith("#")) {
                            stopWords.add(line);
                        }
                    }
                }
            }

            if (stopWords.isEmpty()) {
                throw new IOException("Stopwords set is empty after reading cache files. Check file contents/URIs.");
            }
        }
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String line = value.toString().trim();
                String[] parts = line.split("\t");
                if (parts.length < 3) return;   // expected: ngram \t year \t count ( \t volumeCount )

                String ngram = parts[0].trim();
                String[] words = ngram.split("\\s+");
                if (words.length != 2) return;

                int year;
                long count;
                try {
                    year = Integer.parseInt(parts[1].trim());
                    count = Long.parseLong(parts[2].trim());
                } catch (NumberFormatException e) {
                    return;
                }

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

    public static class PartitionerClass extends Partitioner<Text, LongWritable> {
        @Override
        public int getPartition(Text key, LongWritable value, int numReducers) {
            return (key.hashCode() & Integer.MAX_VALUE) % numReducers;
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
        job.setCombinerClass(BigramDecade.CombinerClass.class);
        job.setReducerClass(BigramDecade.ReducerClass.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.addCacheFile(new java.net.URI(args[2]));
        job.addCacheFile(new java.net.URI(args[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
