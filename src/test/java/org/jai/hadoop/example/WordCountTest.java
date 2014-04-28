package org.jai.hadoop.example;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterMapReduceTestCase;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Utils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Ignore;
import org.junit.Test;

public class WordCountTest extends ClusterMapReduceTestCase
{
//    public void test() throws Exception
//    {
//         JobConf conf = createJobConf();
//         Path inDir = new Path("testing/jobconf/input");
//         Path outDir = new Path("testing/jobconf/output");
//         OutputStream os = getFileSystem().create(new Path(inDir, "text.txt"));
//         Writer wr = new OutputStreamWriter(os);
//         wr.write("b a\n");
//         wr.close();
//         conf.setJobName("mr");
//         conf.setOutputKeyClass(Text.class);
//         conf.setOutputValueClass(LongWritable.class);
////         conf.setMapperClass(WordCountMapper.class);
////         conf.setReducerClass(SumReducer.class);
//         org.apache.hadoop.mapred.FileInputFormat.setInputPaths(conf, inDir);
//         org.apache.hadoop.mapred.FileOutputFormat.setOutputPath(conf, outDir);
//         assertTrue(JobClient.runJob(conf).isSuccessful());
//         // Check the output is as expected
//         Path[] outputFiles = FileUtil.stat2Paths(getFileSystem().listStatus(outDir, new Utils.OutputFileUtils.OutputFilesFilter()));
//         assertEquals(1, outputFiles.length);
//         InputStream in = getFileSystem().open(outputFiles[0]);
//         BufferedReader reader = new BufferedReader(new InputStreamReader(in));
//         assertEquals("a\t1", reader.readLine());
//         assertEquals("b\t1", reader.readLine());
//         assertNull(reader.readLine());
//         reader.close();
//    }

    public void test2() throws Exception
    {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "wordcount");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(SumReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path("file:///home/tom/sample.txt"));
        FileOutputFormat.setOutputPath(job, new Path("blahblah"));
        job.waitForCompletion(true);
    }
    
    @Override
    protected void setUp() throws Exception {

        System.setProperty("hadoop.log.dir", "/tmp/logs");

        super.startCluster(true, null);
    }

    @Test
    public void test3()
    {
        String testName = "trial";
        File baseDir = new File("./target/hdfs/" + testName).getAbsoluteFile();
        FileUtil.fullyDelete(baseDir);
        Configuration conf = new Configuration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
        MiniDFSCluster hdfsCluster = null;
		try {
			hdfsCluster = builder.build();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		}
        String hdfsURI = "hdfs://localhost:" + hdfsCluster.getNameNodePort() + "/";
        System.out.println("HDFS url is " + hdfsURI);
    }

    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);

        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens())
            {
                word.set(tokenizer.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int sum = 0;
            for (IntWritable val : values)
            {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}
