package hadoop.examples;
import java.io.IOException;
import java.util.*;

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

public class LLRCompute {
    public static class MapperClass extends Mapper<LongWritable,Text,Text ,Text> {
        private final Text outKey = new Text();
        private final Text outValue = new Text();


        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            String[] parts = line.split("\t");
            if (parts.length < 7) return;

            String decade = parts[0];
            String w1 = parts[1];
            String w2 = parts[2];
            long c12,c1,c2,N;
            try{
                c12 = Long.parseLong(parts[3]);
                c1 = Long.parseLong(parts[4]);
                c2 = Long.parseLong(parts[5]);
                N = Long.parseLong(parts[6]);
            } catch (NumberFormatException e) {return;}
            long a = c12; //w1*w2
            long b = c1 -c12; // after w1 there isn't w2
            long c = c2 -c12; // before w2 there isn't w1
            long d = N -c1 -c2 +c12; // doesn't include w1 , w2
            if (a < 0 || b < 0 || c < 0 || d < 0) return;
            
            double llr = computeLLR(a,b,c,d);
            outKey.set(decade);
            outValue.set(w1 +"\t" +w2 +"\t" +llr +"\t" +c12 +"\t" +c1 +"\t" +c2 +"\t" +N);
            context.write(outKey,outValue);
        }

        private double computeLLR(long a, long b, long c, long d) {
            double N = (double) a+b+c+d;
            if (N == 0) return 0;
            double row1 = a+b;  // w1 appearances
            double row2 = c+d; // w1 doesn't appear
            double col1 = a+c; // w2 appearances
            double col2 = b+d; // w2 doesn't appear

            double expected11 = row1 * col1/N;
            double expected12 = row1 *col2 /N;
            double expected21 = row2*col1 /N;
            double expected22 = row2*col2 /N;

            double ll =0.0;

            if (a>0 && expected11>0) ll+= a*Math.log(a/expected11);
            if (b>0 && expected12 > 0) ll+= b*Math.log(b/expected12);
            if (c>0 && expected21>0) ll+= c*Math.log(c/expected21);
            if (d>0 && expected22>0) ll+= d*Math.log(d/expected22);

            return 2.0 *ll;
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }

    }




    public static class ReducerClass extends Reducer<Text,Text,Text,Text> {
        private  final Text outKey = new Text();
        private final Text outValue = new Text();
        private int topK;
        private static class BigramRecord{
            String w1;
            String w2;
            double llr;
            long c12;
            long c1;
            long c2;
            long N;
        }

        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
            topK = context.getConfiguration().getInt("topK", 100);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            PriorityQueue<BigramRecord> heap = new PriorityQueue<BigramRecord>(
                    new Comparator<BigramRecord>() {
                        @Override
                        public int compare(BigramRecord a, BigramRecord b) {
                            return Double.compare(a.llr, b.llr);
                        }
                    }
            );

            for (Text value : values) {
                String[] parts = value.toString().split("\t");
                if (parts.length < 7) continue;
                BigramRecord record = new BigramRecord();
                record.w1 =parts[0];
                record.w2 =parts[1];
                try{
                    record.llr = Double.parseDouble(parts[2]);
                    record.c12 = Long.parseLong(parts[3]);
                    record.c1 = Long.parseLong(parts[4]);
                    record.c2 = Long.parseLong(parts[5]);
                    record.N = Long.parseLong(parts[6]);
                }catch (NumberFormatException e) {continue;}
                if (heap.size() < topK) heap.add(record);
                else {
                    if (heap.peek().llr < record.llr) {
                        heap.poll();
                        heap.add(record);
                    }
                }
            }
            List <BigramRecord> best =  new ArrayList<BigramRecord>(heap);
            best.sort(new Comparator<BigramRecord>() {
                @Override
                public int compare(BigramRecord a, BigramRecord b) {
                    return Double.compare(b.llr, a.llr); // makes it in descending order
                }
            });
            String decadeStr = key.toString();
            for (BigramRecord r : best) {
                outKey.set(decadeStr +"\t" +r.w1 +"\t" +r.w2);
                outValue.set(r.llr +"\t" +r.c12 + "\t" +r.c1 +"\t" +r.c2 +"\t" +r.N);
                context.write(outKey,outValue);
            }
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }
    }




    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("error in  args for job 4");
            System.exit(1);
        }
        Configuration conf = new Configuration();
        conf.setInt("topK", 100);
        Job job = Job.getInstance(conf, "LLRCompute");
        job.setJarByClass(LLRCompute.class);

        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
