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
            System.err.println(
                "Usage: JobFlowNgram <region> <bucket> <unigram-path> <bigram-path>");
            System.exit(1);
        }

        String regionName = args[0];        
        String bucket = args[1];        
        String oneGram = args[2];        
        String twoGram = args[3];        
       
        String job1Out  = "s3://" + bucket + "/ngrams/job1";
        String job2Out  = "s3://" + bucket + "/ngrams/job2";
        String job3aOut = "s3://" + bucket + "/ngrams/job3a";
        String job3bOut = "s3://" + bucket + "/ngrams/job3b";
        String job4Out  = "s3://" + bucket + "/ngrams/job4";

     
        String engStop = "s3://" + bucket + "/conf/eng-stopwords.txt";
        String hebStop = "s3://" + bucket + "/conf/heb-stopwords.txt";

        
        String jarPath = "s3://" + bucket + "/jars/hadoop-examples-1.0-SNAPSHOT.jar";

        
        AmazonElasticMapReduce emr = AmazonElasticMapReduceClientBuilder
                .standard()
                .withRegion(Regions.fromName(regionName))
                .withCredentials(new DefaultAWSCredentialsProviderChain())
                .build();

      

        // Job 1: UnigramDecade
        HadoopJarStepConfig job1Jar = new HadoopJarStepConfig()
                .withJar(jarPath)
                .withMainClass("hadoop.examples.UnigramDecade")
                .withArgs(oneGram, job1Out);

        StepConfig job1Step = new StepConfig()
                .withName("unigram-decade")
                .withHadoopJarStep(job1Jar)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // Job 2: BigramDecade (with stopwords)
        HadoopJarStepConfig job2Jar = new HadoopJarStepConfig()
                .withJar(jarPath)
                .withMainClass("hadoop.examples.BigramDecade")
                .withArgs(twoGram, job2Out, engStop, hebStop);

        StepConfig job2Step = new StepConfig()
                .withName("bigram-decade-stopwords")
                .withHadoopJarStep(job2Jar)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // Job 3a: JoinLeftUnigram
        HadoopJarStepConfig job3aJar = new HadoopJarStepConfig()
                .withJar(jarPath)
                .withMainClass("hadoop.examples.JoinLeftUnigram")
                .withArgs(job1Out, job2Out, job3aOut);

        StepConfig job3aStep = new StepConfig()
                .withName("join-left-unigram")
                .withHadoopJarStep(job3aJar)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // Job 3b: JoinRightUnigramAndTotal
        HadoopJarStepConfig job3bJar = new HadoopJarStepConfig()
                .withJar(jarPath)
                .withMainClass("hadoop.examples.JoinRightUnigramAndTotal")
                .withArgs(job3aOut, job1Out, job3bOut);

        StepConfig job3bStep = new StepConfig()
                .withName("join-right-unigram-and-total")
                .withHadoopJarStep(job3bJar)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // Job 4: LLRCompute
        HadoopJarStepConfig job4Jar = new HadoopJarStepConfig()
                .withJar(jarPath)
                .withMainClass("hadoop.examples.LLRCompute")
                .withArgs(job3bOut, job4Out);

        StepConfig job4Step = new StepConfig()
                .withName("llr-compute")
                .withHadoopJarStep(job4Jar)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(2)
                .withMasterInstanceType("m4.large")
                .withSlaveInstanceType("m4.large")
                .withEc2KeyName("vockey")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType(regionName + "a"));



        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("Ngrams-LLR-Pipeline")
                .withReleaseLabel("emr-5.36.0")
                .withInstances(instances)
                .withSteps(job1Step, job2Step, job3aStep, job3bStep, job4Step)
                .withLogUri("s3://" + bucket + "/logs/")
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole");

        RunJobFlowResult result = emr.runJobFlow(runFlowRequest);
        System.out.println("Started EMR job flow with id: " + result.getJobFlowId());
    }
}
