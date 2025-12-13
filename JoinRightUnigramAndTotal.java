package hadoop.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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

public class JoinRightUnigramAndTotal {
    public static class BidramWithLeftMapperClass extends Mapper<LongWritable,Text,Text ,Text> {
        private final Text outKey = new Text();
        private final Text outValue = new Text();


        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            String line = value.toString().trim(); //output of job 3a - JoinLeftUnigram
            if (line.isEmpty()){return;}
            String[] parts = line.split("\t");
            if (parts.length < 5){return;}
            String decade = parts[0];
            String w1 = parts[1];
            String w2 = parts[2];
            String c12 = parts[3];
            String c1 = parts[4];
            outKey.set(decade);
            outValue.set("B\t" +w1+"\t"+w2+"\t"+c12+"\t"+c1);
            context.write(outKey, outValue);
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }

    }

    public static class UnigramAndTotalMapperClass extends Mapper<LongWritable,Text,Text ,Text> {
        private final Text outKey = new Text();
        private final Text outValue = new Text();


        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            String line = value.toString().trim(); // from job 1
            if (line.isEmpty()){return;}
            String[] parts = line.split("\t");
            if (parts.length < 3){return;}
            String decade = parts[0];
            String word = parts[1];
            String count =  parts[2];
            if (word.equals("*TOTAL*")){
                outKey.set(decade);
                outValue.set("N" +"\t" + count); // N stands for total appearances
            }
            else {
                outKey.set(decade);
                outValue.set("U" +"\t" + word + "\t" + count);
            }
            context.write(outKey, outValue);

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
            long N = -1;
            HashMap<String, Long> rightCounts = new HashMap<>();
            List<String> bigrams =  new ArrayList<>();

            for (Text taggedValue : taggedValues) {
                String[] parts = taggedValue.toString().split("\t");
                if (parts.length ==0) continue;
                switch (parts[0]) {
                    case "N":
                        if (parts.length >= 2) {
                            try {
                                N = Long.parseLong(parts[1]);
                            } catch (NumberFormatException ignored) {
                            }
                        }
                        break;

                    case "U":
                        if (parts.length >= 3) {
                            String w2 = parts[1];
                            try {
                                long c2 = Long.parseLong(parts[2]);
                                rightCounts.put(w2, c2);
                            } catch (NumberFormatException ignored) {
                            }
                        }
                        break;

                    case "B":
                        bigrams.add(taggedValue.toString());

                }
            }
            if (N <=0) return ;
            String decade = key.toString();
            for (String rec : bigrams) {
                String[] parts = rec.split("\t");
                if (parts.length  < 5) continue;
                String w1 = parts[1];
                String w2 = parts[2];
                long c12;
                long c1;
                try{
                    c12 = Long.parseLong(parts[3]);
                    c1 = Long.parseLong(parts[4]);
                }catch(NumberFormatException e){continue;}
                Long count2 =  rightCounts.get(w2);
                if (count2 == null){continue;}
                long c2 = count2.longValue();
                outKey.set(decade + "\t" +w1 +"\t" +w2);
                outValue.set(c12 + "\t" +c1 +"\t" +c2 + "\t" +N);
                context.write(outKey, outValue);
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
        Job job = Job.getInstance(conf, "JoinRightUnigramAndTotal");
        job.setJarByClass(JoinRightUnigramAndTotal.class);

        MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class,
                JoinRightUnigramAndTotal.BidramWithLeftMapperClass.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class,
                JoinRightUnigramAndTotal.UnigramAndTotalMapperClass.class);

        job.setReducerClass(JoinRightUnigramAndTotal.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
