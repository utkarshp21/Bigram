//REFERENCED CODE FROM
//http://cutler.io/2012/07/hadoop-processing-zip-files-in-mapreduce/
//https://github.com/cotdp/com-cotdp-hadoop
//https://gist.github.com/jteso/1868049
//https://www.ibm.com/developerworks/library/bd-hadoopcombine/index.html
//https://wikis.nyu.edu/display/NYUHPC/Big+Data+Tutorial+1%3A+MapReduce

package bigram;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class BiagramReducer
        extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
    ) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}
