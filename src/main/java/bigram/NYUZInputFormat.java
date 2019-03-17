package bigram;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**

 */
public class NYUZInputFormat extends FileInputFormat<LongWritable, Text> {

    public static final String CONFIG_MEMBER_NAME = "multilinejsoninputformat.member";

    @Override
    public RecordReader<LongWritable, Text>
    createRecordReader(InputSplit split,
                       TaskAttemptContext context) throws IOException {
        String member = HadoopCompat.getConfiguration(context).get(CONFIG_MEMBER_NAME);

        if (member == null) {
            throw new IOException("Missing configuration value for " + CONFIG_MEMBER_NAME);
        }
        return new NYUZRecordReader(member);
    }

    public static void setInputJsonMember(Job job, String member) {
        HadoopCompat.getConfiguration(job).set(CONFIG_MEMBER_NAME, member);
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        CompressionCodec codec =
                new CompressionCodecFactory(HadoopCompat.getConfiguration(context)).getCodec(file);
        return codec == null;
    }

}


