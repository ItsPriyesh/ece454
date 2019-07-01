import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task4 {

    /*
        The Lord of The Rings,5,4,4,,3,2
        Apocalypto,3,5,4,,5,4
        Apollo 13,,,4,5,,5
     */

    private static final String DISTRIBUTED_CACHE_LABEL = "cachedInput";

    public static class SimilarityMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final Text titlePair = new Text();
        private final IntWritable similarity = new IntWritable();

        private final List<String> cache = new LinkedList<>();

        @Override
        protected void setup(Context context) throws IOException {
            BufferedReader reader = new BufferedReader(new FileReader(DISTRIBUTED_CACHE_LABEL));
            cache.clear();
            String line;
            while ((line = reader.readLine()) != null) cache.add(line);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            final String[] tokensA = value.toString().split(",", -1);
            final String titleA = tokensA[0];
            for (String cacheLine : cache) {
                final String[] tokensB = cacheLine.split(",", -1);
                final String titleB = tokensB[0];

                // maintain lexicographic order between titles
                if (titleA.compareTo(titleB) >= 0) continue;

                int count = 0;
                for (int i = 1; i < tokensA.length; i++) {
                    String a = tokensA[i];
                    String b = tokensB[i];
                    if (!a.isEmpty() && !b.isEmpty() && a.equals(b)) {
                        count++;
                    }
                }
                titlePair.set(titleA + "," + titleB);
                similarity.set(count);
                context.write(titlePair, similarity);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf, "Task4");
        job.setJarByClass(Task4.class);

        // Add the input to the distributed cache so each worker has a local cached copy
        job.addCacheFile(new URI(otherArgs[0] + "#" + DISTRIBUTED_CACHE_LABEL));

        job.setMapperClass(SimilarityMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
        TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
