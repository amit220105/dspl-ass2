package hadoop.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class JoinLeftUnigram {
    public static class UnigramMapperClass extends Mapper<LongWritable,Text,Text ,Text> {
        private final Text outKey = new Text();
        private final Text outValue = new Text();


        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()){return;}

            String[] parts = line.split("\t");
            if (parts.length <3) {return;}

            String decade = parts[0];
            String word = parts[1];
            String count = parts[2];

            if (word.equals("*TOTAL*")) {return;}

            outKey.set(decade + "\t" + word); //join key
            outValue.set("U\t" + count);
            context.write(outKey,outValue);

        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }

    }

    public static class BigramMapperClass extends Mapper<LongWritable,Text,Text ,Text> {
        private final Text outKey = new Text();
        private final Text outValue = new Text();


        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()){return;}

            String[] parts = line.split("\t");
            if (parts.length <4) {return;}

            String decade = parts[0];
            String w1 = parts[1];
            String w2 = parts[2];
            String count = parts[3];


            outKey.set(decade + "\t" + w1); // join key
            outValue.set("B\t"+ w2 + "\t" + count);
            context.write(outKey,outValue);
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }

    }


    public static class ReducerClass extends Reducer<Text,Text,Text,Text> {
        private  final Text outKey = new Text();
        private final Text outValue = new Text();

        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void reduce(Text key, Iterable<Text> taggedValues, Context context) throws IOException,  InterruptedException {
            long c1 = -1;
            List<String>  bigrams = new ArrayList<String>();
            for (Text val : taggedValues) {
                String[] parts = val.toString().split("\t");
                if (parts.length < 2) {continue;}
                if (parts[0].equals("U")) {
                    try {
                        c1 = Long.parseLong(parts[1]);
                    }catch (NumberFormatException ignored){}
                }else if (parts[0].equals("B")) {
                    bigrams.add(val.toString());
                }

            }
            if (c1 <0){return;}

            String[] keyParts = key.toString().split("\t");
            if (keyParts.length < 2) {return;}
            String decade = keyParts[0];
            String w1  = keyParts[1];

            for (String val : bigrams) {
                String[] parts = val.toString().split("\t");
                String w2  = parts[1];
                String c12 = parts[2];
                outKey.set(decade+ "\t" + w1 + "\t" + w2);
                outValue.set(c12 + "\t" + c1);
                context.write(outKey,outValue);
            }

        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }
    }




    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: JoinLeftUnigram <unigrams-in> <bigrams-in> <output>");
            System.exit(1);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "JoinLeftUnigram");
        job.setJarByClass(JoinLeftUnigram.class);

        MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class, UnigramMapperClass.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, BigramMapperClass.class);

        job.setReducerClass(JoinLeftUnigram.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}


