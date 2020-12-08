import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Main {
    public static final String HDFS_url = "hdfs://hadoop-node1:9000";

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Map<String, String> path = new HashMap<>();
        // 这版程序是直接放在 Linux 上跑的，中间的计算结果也直接放在了 HDFS 上
//        path.put("input1", HDFS_url + "/hbase/data/movie/movieLens");
//        path.put("output1", HDFS_url + "/data/output1");
        path.put("input2", path.get("output1"));
        path.put("output2", HDFS_url + "/data/output2");
        path.put("input3", path.get("output1"));
        path.put("output3", HDFS_url + "/data/output3");
        path.put("input4_1", path.get("output2"));
        path.put("input4_2", path.get("output3"));
        path.put("output4", HDFS_url + "/data/output4");
        Step1.run(path);
        Step2.run(path);
        Step3.run(path);
        Step4.run(path);

    }
}
