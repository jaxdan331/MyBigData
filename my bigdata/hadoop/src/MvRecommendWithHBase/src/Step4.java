import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class Step4 {
    public static class Step4Mapper extends Mapper<LongWritable, Text, Text, Text> {
        private static Text k = new Text();
        private static Text v = new Text();

        protected String getInputPath(Context context) throws IOException, InterruptedException {
            InputSplit inputSplit = context.getInputSplit();
            // String inputFileName = ((FileSplit)inputSplit).getPath().getName();  // 获取文件名
            String inputFilePath = ((FileSplit)inputSplit).getPath().getParent().toUri().getPath(); // 获取文件路径
            // System.out.println(inputFilePath + ' ' + inputFileName);
            String[] path = inputFilePath.split("/");
            return  path[path.length - 1];  // Java 里面只能这样获取数组最后一个元素，不能用 -1
        }
        
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String inputPath = this.getInputPath(context);
            System.out.println(inputPath);
            String[] tokens = value.toString().split("\t");
            // 其实读取文件的时候肯定先是读 output2 中的，然后再读 output3 中的
            // 所以在 else 执行的时候同现矩阵 cooccurMat 其实已经建立完成了
            String[] vals = value.toString().split("\t");
            v.set(vals[0] + ',' + vals[1]);
            if (inputPath.equals("output2")) {
                k.set("CM");  // 同现矩阵标志
                context.write(k, v);
            }else if(inputPath.equals("output3")) {
                k.set("RL");  // 评分列表标志
                context.write(k, v);
            }
        }
    }

    public static class Step4Reducer extends Reducer<Text, Text, IntWritable, Text> {
        private static IntWritable k = new IntWritable();
        private static Text v = new Text();
        // 用于存储同现矩阵，为了减少内存的占用，这里使用列表来存储矩阵
        private static Map<Integer, List<Cooccurence>> cooccurMat = new HashMap<>();
        private static Iterable<Text> out3;  // 缓存评分矩阵
        private static boolean haveBuiltMat = false;  // 记录一下是否已经建立好了同现矩阵和评分矩阵
        private static Map<Integer, List<String>> results = new HashMap<>();  // 用来存储中间结果

        private void write(Context context) throws IOException, InterruptedException {  // 合并同现矩阵与评分矩阵
            for (Text val : out3) {
                // output3 经 map 操作后每一条数据的格式为：“mvId,usrId:rate”
                // System.out.println("val: " + val.toString());
                String[] items = val.toString().split(",");  // mv,usr:rate -> mv usr:rate
                String[] mv_rt = items[1].split(":");
                int mvId = Integer.parseInt(items[0]);
                int usrId = Integer.parseInt(mv_rt[0]);
                double rate = Double.parseDouble(mv_rt[1]);

                // System.out.println("usrId: " + usrId + ", mvId: " + mvId + ", rate: " + rate);

                for (Cooccurence co : cooccurMat.get(mvId)) {
                    // 输出形式："usrId movie2,rate"，其中 movie2 是给用户推荐的电影，rate 是推荐指数（打分）
                    String value = co.getItemID2() + "," + rate * co.getNum();
                    // System.out.println(value);
                    List<String> list;
                    if (!results.containsKey(usrId)) {
                        list = new ArrayList<>();
                    }else {
                        list = results.get(usrId);
                    }
                    list.add(value);
                    results.put(usrId, list);
                }
            }
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println(key.toString());
            if (key.toString().equals("CM")) {  // 建立同现矩阵
                for (Text val : values) {
                    String[] items = val.toString().split(",");  // mv1:mv2,n -> mv1:mv2 n
                    String[] mvPair = items[0].split(":");
                    int mvId1 = Integer.parseInt(mvPair[0]);
                    int mvId2 = Integer.parseInt(mvPair[1]);
                    int num = Integer.parseInt(items[1]);
                    List<Cooccurence> list;
                    if (!cooccurMat.containsKey(mvId1)) {
                        list = new ArrayList<>();
                    }else {
                        list = cooccurMat.get(mvId1);
                    }
                    list.add(new Cooccurence(mvId1, mvId2, num));
                    cooccurMat.put(mvId1, list);
                }
                System.out.println("Mat size: " + cooccurMat.size());
                haveBuiltMat = true;
                if (out3 != null) {
                    this.write(context);
                }
            } else {  // 获得评分矩阵
                out3 = values;  // 变量是 static 的，不能使用 'this.'
                if (haveBuiltMat) {
                    this.write(context);
                }
            }
            // 两个矩阵都建立好了以后（不管是谁先先建立谁后建立），在最后一个建立好了之后都会执行这一步
            if (!results.isEmpty()) {
                Map<String, Double> result = new HashMap<>();
                for (Text val : values) {
                    String[] str = val.toString().split(",");
                    // System.out.println(str[0] + ' ' + str[1]);
                    if (result.containsKey(str[0])) {
                        result.put(str[0], result.get(str[0]) + Double.parseDouble(str[1]));
                    }else {
                        result.put(str[0], Double.parseDouble(str[1]));
                    }
                }
                for (Map.Entry<Integer, List<String>> entry : results.entrySet()) {
                    k.set(entry.getKey());
                    for (String val : entry.getValue()) {
                        String[] str = val.split(",");  // val: mvId2,rate*num
                        if (result.containsKey(str[0])) {
                            result.put(str[0], result.get(str[0]) + Double.parseDouble(str[1]));
                        }else {
                            result.put(str[0], Double.parseDouble(str[1]));
                        }

                    }
                    for (String mvId2 : result.keySet()) {
                        double score = result.get(mvId2);
                        v.set(mvId2 + "," + score);
                        context.write(k, v);
                    }
                }
            }
        }
    }

    public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Step4");
        job.setJarByClass(Step4.class);

        job.setMapperClass(Step4Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(Step4Reducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        // map 函数读取文件的时候，先 add 到输入路径中的先读
        FileInputFormat.addInputPath(job, new Path(path.get("input4_1")));
        FileInputFormat.addInputPath(job, new Path(path.get("input4_2")));
        FileOutputFormat.setOutputPath(job, new Path(path.get("output4")));

        job.waitForCompletion(true);
    }
}

