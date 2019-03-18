//REFERENCED CODE FROM
//http://cutler.io/2012/07/hadoop-processing-zip-files-in-mapreduce/
//https://github.com/cotdp/com-cotdp-hadoop
//https://gist.github.com/jteso/1868049
//https://www.ibm.com/developerworks/library/bd-hadoopcombine/index.html
//https://wikis.nyu.edu/display/NYUHPC/Big+Data+Tutorial+1%3A+MapReduce

package bigram;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class NYUZRecordReader
        extends RecordReader<Text, BytesWritable>
{
    private FSDataInputStream fsin;

    private ZipInputStream zip;

    private Text currentKey;

    private BytesWritable currentValue;

    private boolean isFinished = false;


    @Override
    public void initialize( InputSplit inputSplit, TaskAttemptContext taskAttemptContext )
            throws IOException, InterruptedException
    {
        FileSplit split = (FileSplit) inputSplit;
        Configuration conf = taskAttemptContext.getConfiguration();
        Path path = split.getPath();
        FileSystem fs = path.getFileSystem( conf );

        fsin = fs.open( path );
        zip = new ZipInputStream( fsin );
    }


    @Override
    public boolean nextKeyValue()
            throws IOException, InterruptedException
    {
        ZipEntry entry = null;
        try
        {
            entry = zip.getNextEntry();
        }
        catch ( ZipException e )
        {
            if ( NYUZInputFormat.getLenient() == false )
                throw e;
        }


        if ( entry == null )
        {
            isFinished = true;
            return false;
        }

        currentKey = new Text( entry.getName() );

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] temp = new byte[8192];
        while ( true )
        {
            int bytesRead = 0;
            try
            {
                bytesRead = zip.read( temp, 0, 8192 );
            }
            catch ( EOFException e )
            {
                if(NYUZInputFormat.getLenient() == false)
                    throw e;
                return false;
            }
            if ( bytesRead > 0 )
                bos.write( temp, 0, bytesRead );
            else
                break;
        }
        zip.closeEntry();

        // Uncompressed contents
        currentValue = new BytesWritable( bos.toByteArray() );
        return true;
    }


    @Override
    public float getProgress()
            throws IOException, InterruptedException
    {
        return isFinished ? 1 : 0;
    }


    @Override
    public Text getCurrentKey()
            throws IOException, InterruptedException
    {
        return currentKey;
    }


    @Override
    public BytesWritable getCurrentValue()
            throws IOException, InterruptedException
    {
        return currentValue;
    }


    @Override
    public void close()
            throws IOException
    {
        try { zip.close(); } catch ( Exception ignore ) { }
        try { fsin.close(); } catch ( Exception ignore ) { }
    }
}
