package bigram;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BigramMapper
        extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Text Bigram = new Text();

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {

        String line = value.toString();
        line = line.replaceAll("[^a-zA-Z0-9]"," ").toLowerCase().trim();

        StringTokenizer itr = new StringTokenizer(line);
        Text prev = null;
        while (itr.hasMoreTokens()) {
            Text w = new Text(itr.nextToken());
            if(prev != null){
                Bigram.set(prev+" "+w);
                context.write(Bigram, one);
            }
            prev = w;
        }

    }
}
