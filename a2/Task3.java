import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task3 {

  /*
      The Lord of The Rings,5,4,4,,3,2
      Apocalypto,3,5,4,,5,4
      Apollo 13,,,4,5,,5
   */

  public static class UserRatingMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

    private final static IntWritable ONE = new IntWritable(1);
    private final static IntWritable ZERO = new IntWritable(0);

    private final IntWritable user = new IntWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      final String[] tokens = value.toString().split(",", -1);
      for (int i = 1; i < tokens.length; i++) {
        user.set(i);
        context.write(user, tokens[i].isEmpty() ? ZERO : ONE);
      }
    }
  }

  public static class UserRatingSumReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    private final IntWritable sum = new IntWritable();

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int numRatings = 0;
      for (IntWritable val : values) {
        numRatings += val.get();
      }
      sum.set(numRatings);
      context.write(key, sum);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", ",");

    Job job = Job.getInstance(conf, "Task3");
    job.setJarByClass(Task3.class);

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    // add code here
    job.setMapperClass(UserRatingMapper.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(IntWritable.class);


    job.setCombinerClass(UserRatingSumReducer.class);
    job.setReducerClass(UserRatingSumReducer.class);

    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
