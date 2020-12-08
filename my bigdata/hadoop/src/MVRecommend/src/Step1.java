import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Step1 {
    public static class Step1Mapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private static IntWritable k = new IntWritable();
        private static Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] items = value.toString().split("\t");
            int usrId = Integer.parseInt(items[0]);
            String movieId = items[1];
            String rate = items[2];
            k.set(usrId);
            v.set(movieId + ':' + rate);
            context.write(k, v);
        }
    }

    public static class Step1Reducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        private static Text v = new Text();

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Text val : values) {
                sb.append(',' + val.toString());
            }
            // 只有 replaceFirst，没有 replaceLast!!
            v.set(sb.toString().replaceFirst(",", ""));
            context.write(key, v);
        }
    }

    public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Step1");
        job.setJarByClass(Step1.class);

        // 设置输入路径
        job.setMapperClass(Step1Mapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(Step1Reducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(path.get("input1")));
        FileOutputFormat.setOutputPath(job, new Path(path.get("output1")));

        job.waitForCompletion(true);
    }
}
