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

import java.io.IOException;
import java.util.Map;


public class Step2 {
    public static class Step2Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static Text k = new Text();
        private static IntWritable v = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 需要注意的是，输出文件中的 key 和 value 之间是 \t 不是空格
            // 所以这里直接使用 \t 和 , 把用户id和"电影id:评分"，以及相邻的"电影id:评分"之间都断开了，第一项是用户id
            String[] items = value.toString().split("[\t,]");
            // 找到一个 movie mi，因为 items[0] 是用户id 所以这里 i 从 1 开始
            for (int i = 1; i < items.length; i++) {
                String movieID = items[i].split(":")[0];
                // 找到与 mi 在同一个用户的观影列表中出现过的 movie mj（包括 mi 自己）
                // 但是要注意，他这里做的是 n2 次的运算，不是 n(n+1)/2 次，也就是说统计过 mi:mj 后，还会 mj:mi 也会再统计一次！！
                // 不过算法的复杂度倒都是 O(n2)
                for (int j = 1; j < items.length; j++) {
                    String movieID2 = items[j].split(":")[0];
                    k.set(movieID + ":" + movieID2);
                    // 看来使用 output.collect 一次可以向上下文中写入多个 k-v 键值对
                    context.write(k, v);
                }
            }
        }
    }

    public static class Step2Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private static IntWritable v = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();  // 使用 get 方法才能把 IntWritable 转换成 int
            }
            v.set(sum);
            context.write(key, v);
        }
    }

    public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Step2");
        job.setJarByClass(Step2.class);

        job.setMapperClass(Step2Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(Step2Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(path.get("input2")));
        FileOutputFormat.setOutputPath(job, new Path(path.get("output2")));

        job.waitForCompletion(true);
    }
}
