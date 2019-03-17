package bigram;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class BigramCount {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "word count");

        job.setJarByClass(BigramCount.class);

        job.setMapperClass(BigramMapper.class);
        job.setCombinerClass(BiagramReducer.class);
        job.setReducerClass(BiagramReducer.class);

        job.setInputFormatClass(NYUZInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //Testing
        NYUZInputFormat.setLenient( true );
        NYUZInputFormat.setInputPaths(job, new Path(args[0]));
        //Testing End

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
