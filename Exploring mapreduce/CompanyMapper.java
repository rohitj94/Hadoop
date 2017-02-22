import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

class CompanyMapper extends Mapper<LongWritable, Text, NullWritable, Text>
{
  //Company c = new Company();
  Text companyName = new Text();
  Text productName = new Text();
  
  /*static enum CustomCounters
  {
	  TRUE, FALSE
  }*/
  
  IntWritable in = new IntWritable(1);

  public void map(LongWritable offset, Text value, Context context) throws IOException, InterruptedException
  {
    String[] line = value.toString().split("\\|");
	companyName.set(line[0]);
    productName.set(line[1]);

    if(!("NA".equalsIgnoreCase(companyName.toString()) || "NA".equalsIgnoreCase(productName.toString())))
    {
      //context.getCounter(CustomCounters.TRUE).increment(1);
      //c.set(NullWritable.get(),value);
      context.write(NullWritable.get(),value);
    }
  }
}
