package bigram;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.mapreduce.Mapper;

public class BigramMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);

    private Text Bigram = new Text();

    public void map(LongWritable key, Text value, Context context
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
