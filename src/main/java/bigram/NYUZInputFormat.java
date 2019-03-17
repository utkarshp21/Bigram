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

/**
 * Extends the basic FileInputFormat class provided by Apache Hadoop to accept ZIP files. It should be noted that ZIP
 * files are not 'splittable' and each ZIP file will be processed by a single Mapper.
 */
public class NYUZInputFormat extends FileInputFormat<Text, BytesWritable> {
    /** See the comments on the setLenient() method */
    private static boolean isLenient = false;

    /**
     * ZIP files are not splitable
     */
    @Override
    protected boolean isSplitable( JobContext context, Path filename )
    {
        return false;
    }

    /**
     * Create the ZipFileRecordReader to parse the file
     */
    @Override
    public RecordReader<Text, BytesWritable> createRecordReader( InputSplit split, TaskAttemptContext context )
            throws IOException, InterruptedException
    {
        return new NYUZRecordReader();
    }

    /**
     *
     * @param lenient
     */
    public static void setLenient( boolean lenient )
    {
        isLenient = lenient;
    }

    public static boolean getLenient()
    {
        return isLenient;
    }
}

//import java.io.IOException;
//import java.util.List;
//import java.util.Enumeration;
//
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.BytesWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.*;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import java.util.ArrayList;
//
//import java.io.InputStream;
//import org.apache.hadoop.fs.FSDataInputStream;
//import org.apache.hadoop.mapreduce.lib.input.FileSplit;
//
//import java.util.zip.ZipInputStream;
//import java.util.zip.ZipFile;
//import java.util.zip.ZipEntry;
//
//
//import org.apache.hadoop.mapreduce.lib.input.FileSplit;


/**
 * Extends the basic FileInputFormat to accept ZIP files.
 * ZIP files are not 'splittable', so we need to process/decompress in place:
 * each ZIP file will be processed by a single Mapper; we are parallelizing files, not lines...
 */



//public class NYUZInputFormat extends InputFormat<Text, BytesWritable> {
//
//    @Override
//    public List<InputSplit> getSplits(JobContext context)
//            throws IOException, InterruptedException {
//
//        ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
//
//        try {
//            ZipFile zipFile = new ZipFile("/user/up293/cookbook_text.zip");
//            Enumeration<? extends ZipEntry> entries = zipFile.entries();
//            while(entries.hasMoreElements()){
//                ZipEntry entry = entries.nextElement();
//                Path filePath=new Path(entry.getName());
//                splits.add(new FileSplit(filePath,0, entry.getSize(),null));
//                System.out.println(new FileSplit(filePath,0, entry.getSize(),null));
//            }
//            System.out.println(splits);
//        }
//        catch(IOException e) {
//            e.printStackTrace();
//        }
//
//        // your code here
//        // our splits will consist of whole files found insize our input zip file
//        // return splits....
//
//        // Create a number of input splits equivalent to the number of tasks
//
//
//        return splits;
//    }
//
//    /*** return a record reader
//     *
//     * @param split
//     * @param context
//     * @return (Text,BytesWritable)
//     * @throws IOException
//     * @throws InterruptedException
//     */
//    @Override
//    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
//            throws IOException, InterruptedException {
//        // no need to modify this one....
//        return new NYUZRecordReader();
//    }
//}
