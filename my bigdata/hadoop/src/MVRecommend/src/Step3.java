import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Step3 {
    public static class Step3Mapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private static IntWritable k = new IntWritable();
        private static Text v = new Text();

        @Override
        // 注意，这个 map 的输入是 step1 的输出
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] items = value.toString().split("[\t,]");
            // items[0] 是用户id
            for (int i = 1; i < items.length; i++) {
                String[] item = items[i].split(":");
                int movieId = Integer.parseInt(item[0]);
                String rate = item[1];
                k.set(movieId);
                v.set(items[0] + ':' + rate);
                context.write(k, v);
//                System.out.println(v.toString());
            }
        }
    }

    // 如果 Reduce 的输出结果与 Mapper 的一样，那么 Reducer 可以不要
//    public static class Step3Reducer extends Reducer<IntWritable, Text, IntWritable, Text> {
//        private static Text v = new Text();
//
//        @Override
//        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            StringBuilder sb = new StringBuilder();
//            for (Text val : values) {
//                sb.append(',' + val.toString());
//            }
//            // 只有 replaceFirst，没有 replaceLast!!
//            v.set(sb.toString().replaceFirst(",", ""));
//            context.write(key, v);
//        }
//    }

    public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Step3");
        job.setJarByClass(Step3.class);

        job.setMapperClass(Step3.Step3Mapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

//        job.setReducerClass(Step3.Step3Reducer.class);
//        job.setOutputKeyClass(IntWritable.class);
//        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(path.get("input3")));
        FileOutputFormat.setOutputPath(job, new Path(path.get("output3")));

        job.waitForCompletion(true);
    }
}
