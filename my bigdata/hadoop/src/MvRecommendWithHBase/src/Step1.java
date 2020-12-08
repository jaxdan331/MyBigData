import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.List;
import java.util.Map;


public class Step1 {
    public static class Step1Mapper extends Mapper<ImmutableBytesWritable, Result, IntWritable, Text> {
        private static IntWritable k = new IntWritable();
        private static Text v = new Text();

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            // 如果输入源是（HDFS上的）文件，输入到 map 中的 value 应该是一行文本，现在换成了 HBase，输入到 map 中的 value 应该变成了一行 cell
            List<Cell> cells = value.listCells();
            int usrId = 0;
            String mvId = "", rate = "";
            for (Cell cell : cells) {
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                String val = Bytes.toString(CellUtil.cloneValue(cell));
                if (qualifier.equals("usrId")) {
                    usrId = Integer.parseInt(val);
                }else if (qualifier.equals("mvId")) {
                    mvId = val;
                }else if (qualifier.equals("rate")) {
                    rate = val;
                }
                k.set(usrId);
                v.set(mvId + ':' + rate);
                context.write(k, v);
            }
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
        conf.set(TableInputFormat.INPUT_TABLE, "movie:movieLens");
        conf.set("fs.defaultFS", "hdfs://hadoop-node1:9000");
        conf.set("hbase.zookeeper.quorum", "hadoop-node1,hadoop-node2,hadoop-node3");

        Job job = Job.getInstance(conf, "Step1");
        job.setJarByClass(Step1.class);

        job.setMapperClass(Step1Mapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(Step1Reducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("/hbase/data/movie/movieLens"));
        job.setInputFormatClass(TableInputFormat.class);  // 这句话很重要
        FileOutputFormat.setOutputPath(job, new Path("/MvOutput/output1"));

        job.waitForCompletion(true);
    }
}
