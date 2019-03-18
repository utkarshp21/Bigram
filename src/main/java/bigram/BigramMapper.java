//REFERENCED CODE FROM
//http://cutler.io/2012/07/hadoop-processing-zip-files-in-mapreduce/
//https://github.com/cotdp/com-cotdp-hadoop
//https://gist.github.com/jteso/1868049
//https://www.ibm.com/developerworks/library/bd-hadoopcombine/index.html
//https://wikis.nyu.edu/display/NYUHPC/Big+Data+Tutorial+1%3A+MapReduce

package bigram;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;


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
