package hadoop.examples;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
public class JobFlowNgram {

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println( "args are not passed correctly");
            System.exit(1);
        }
        String region = args[0];
        String bucket = args[1];
        String oneGram = args[2];
        String twoGram = args[3];

        String job1Out = "s3://" + bucket + "/ngrams/job1";
        String job2Out = "s3://" + bucket + "/ngrams/job2";
        String job3aOut = "s3://" + bucket + "/ngrams/job3a";
        String job3bOut = "s3://" + bucket + "/ngrams/job3b";
        String job4Out = "s3://" + bucket + "/ngrams/job4";

        String engStop =  "s3://" + bucket + "/conf/eng-stopwords.txt";
        String hebStop =  "s3://" + bucket + "/conf/heb-stopwords.txt";

        String jarPath = "s3://" + bucket + "/jars/hadoop-examples-1.0-SNAPSHOT.jar";

        HadoopJarStepConfig job1Jar = new HadoopJarStepConfig()
                .withJar(jarPath)
                .withMainClass("hadoop.examples.UnigramDecade")
                .withArgs(oneGram, job1Out);



    }
}
