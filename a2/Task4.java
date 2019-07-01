import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
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

    public static class SimilarityMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final Text titlePair = new Text();
        private final IntWritable similarity = new IntWritable();

        private final Map<String, String[]> cache = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            final URI cacheFile = context.getCacheFiles()[0];
            cache.clear();

            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.getLocal(conf);

            String line;
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(cacheFile))));

            while ((line = reader.readLine()) != null) {
                final String[] tokens = line.split(",", -1);
                final String title = tokens[0];
                final String[] users = Arrays.copyOfRange(tokens, 1, tokens.length);
                cache.put(title, users);
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            final String[] tokens = value.toString().split(",", -1);
            final String titleA = tokens[0];
            for (Map.Entry<String, String[]> entry : cache.entrySet()) {
                final String titleB = entry.getKey();

                // maintain lexicographic order between titles
                if (titleA.compareTo(titleB) >= 0) continue;

                int count = 0;
                final String[] usersB = entry.getValue();
                for (int i = 0; i < usersB.length; i++) {
                    String a = tokens[i + 1];
                    String b = usersB[i];
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

        // Add the input to the distributed cache so each worker has a cache copy
//        System.out.println(Arrays.toString(fs.listStatus(new Path(File.separator + "user/p99patel/" ))));
        Path in = new Path(File.separator + "user/p99patel" + otherArgs[0]);

        FileSystem fs = FileSystem.get(conf);
//        FSDataOutputStream outputStream = fs.create(in);
//
//        BufferedReader reader = new BufferedReader(new FileReader(new File(otherArgs[0])));
//        String line;
//        while ((line = reader.readLine()) != null) {
//            outputStream.writeUTF(line);
//        }


        System.out.println(Arrays.toString(fs.listStatus(new Path(File.separator + "a2_inputs"))));

        job.addCacheFile(in.toUri());

        job.setMapperClass(SimilarityMapper.class);

        TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
        TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
