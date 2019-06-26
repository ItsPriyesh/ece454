import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task1 {

    // add code here
    public static class MaxRatingMapper extends Mapper<Object, Text, Text, Text> {

        private Text title = new Text();
        private Text maxUsers = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            final String[] tokens = value.toString().split(",", -1);
            title.set(tokens[0]);

            int maxRating = 0;
            for (int i = 1; i < tokens.length; i++) {
                if (tokens[i].isEmpty()) continue;

                int rating = Integer.parseInt(tokens[i]);
                if (rating > maxRating) {
                    maxRating = rating;
                    maxUsers.clear();
                    maxUsers.set(String.valueOf(i));
                } else if (rating == maxRating) {
                    byte[] bytes = String.format(",%s", i).getBytes();
                    maxUsers.append(bytes, 0, bytes.length);
                }
            }
            context.write(title, maxUsers);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        Job job = Job.getInstance(conf, "Task1");
        job.setJarByClass(Task1.class);

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // add code here

        job.setMapperClass(MaxRatingMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
        TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
