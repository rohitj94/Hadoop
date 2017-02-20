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

public class HDFSJavaProgrmTask2 {

    public static List<String> getPath(Path filePath, FileSystem fs) throws FileNotFoundException, IOException
    {
        List<String> fileList = new ArrayList<String>();
       
        FileStatus[] fileStatus = fs.listStatus(filePath);
       
        for (FileStatus fileStat : fileStatus) {
            if (fileStat.isDirectory()) {
                fileList.addAll(getPath(fileStat.getPath(), fs));
            } else {
                fileList.add(fileStat.getPath().toString());
            }
        }
        return fileList;
    }
   
    public static void main(String args[]) throws IOException, URISyntaxException
    {
        Configuration conf = new Configuration();
       
        List<String> Listing = getPath(new Path("hdfs://localhost:9000/"),FileSystem.get(conf));
          
        for (int i=0;i < Listing.size();i++)
        {
              System.out.println(Listing.get(i));   
        }
    }
}