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

import org.apache.hadoop.io.BytesWritable;

public class BigramMapper extends Mapper<Text, BytesWritable, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);

    private Text Bigram = new Text();

    public void map(Text key, BytesWritable value, Context context
    ) throws IOException, InterruptedException {

        String line = new String( value.getBytes(), "UTF-8" );

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
