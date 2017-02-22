
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//import org.apache.hadoop.mapreduce.v2.api.records.Counter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counters;

public class TelevisionDriver extends Configured implements Tool {
    @SuppressWarnings("deprecation")
    
    
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "TVDetails");
        job.setJarByClass(TelevisionDriver.class);
        

        job.setMapperClass(CompanyMapper.class);
       // job.setReducerClass(CompanyReducer.class);
       
        job.setNumReduceTasks(0);
        
        //job.setMapOutputKeyClass(Company.class);
        //job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
        //System.out.println("CounterValue" + job.getCounters().findCounter(CustomCounters.TRUE).getValue());
        
        /*job.setPartitionerClass(CompanyPartitioner.class);
        job.setGroupingComparatorClass(CompanyComparator.class);
        job.setSortComparatorClass(CompanyProductComparator.class);*/
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        
        return job.waitForCompletion(true) ? 0 : 1;
        		
        /*
        Path out=new Path(args[1]);
        out.getFileSystem(conf).delete(out);
        */
       
    }
    
    public static void main(String args[]) throws Exception
    {
    	int exitCode = ToolRunner.run(new TelevisionDriver(), args);
    }
}
