import java.io.IOException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.net.*;
import org.apache.hadoop.fs.Path;
import java.util.ArrayList;
import java.util.List;
import java.io.FileNotFoundException;

public class HDFSJavaProgrm
{
   
    public static void main(String args[]) throws IOException, URISyntaxException
    {
        Configuration conf = new Configuration();
       
        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000/"),conf);
       
        FileStatus[] fileStatus = fs.listStatus(new Path("hdfs://localhost:9000/"));
       
        Path[] paths = FileUtil.stat2Paths(fileStatus);
       
        for (Path p : paths)
        {
          System.out.println(p.toString());   
        }
    }
}