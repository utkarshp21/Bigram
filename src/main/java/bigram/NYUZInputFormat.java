//REFERENCED CODE FROM
//http://cutler.io/2012/07/hadoop-processing-zip-files-in-mapreduce/
//https://github.com/cotdp/com-cotdp-hadoop
//https://gist.github.com/jteso/1868049
//https://www.ibm.com/developerworks/library/bd-hadoopcombine/index.html
//https://wikis.nyu.edu/display/NYUHPC/Big+Data+Tutorial+1%3A+MapReduce

package bigram;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class NYUZInputFormat extends FileInputFormat<Text, BytesWritable> {
    private static boolean isLenient = false;


    @Override
    protected boolean isSplitable( JobContext context, Path filename )
    {
        return false;
    }


    @Override
    public RecordReader<Text, BytesWritable> createRecordReader( InputSplit split, TaskAttemptContext context )
            throws IOException, InterruptedException
    {
        return new NYUZRecordReader();
    }

    public static void setLenient( boolean lenient )
    {
        isLenient = lenient;
    }

    public static boolean getLenient()
    {
        return isLenient;
    }
}

