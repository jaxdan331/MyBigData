import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Main {
    // 这是 HDFS 的 IP 地址，写成 hdfs://hadoop-node1:9000 应该也可以
    // public static final String HDFS_url = "hdfs://hadoop-node1:9000";

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Map<String, String> path = new HashMap<>();

        if (args.length != 2) {  // 这版是在本地测试程序
            System.out.println("Local mode.");
            // System.exit(1);
            path.put("input1", "./data/input/movie_test");
            path.put("output1", "./data/output1");
            path.put("input2", path.get("output1"));
            path.put("output2", "./data/output2");
            path.put("input3", path.get("output1"));
            path.put("output3", "./data/output3");
            path.put("input4_1", path.get("output2"));
            path.put("input4_2", path.get("output3"));
            path.put("output4", "./data/output4");
        }else {  // 这版程序是直接放在 Linux 上跑的，中间的计算结果也直接放在了 HDFS 上
            System.out.println("Cluster mode.");
            path.put("input1", args[0]);
            path.put("output1", "/output/output1");
            path.put("input2", path.get("output1"));
            path.put("output2", "/output/output2");
            path.put("input3", path.get("output1"));
            path.put("output3", "/output/output3");
            path.put("input4_1", path.get("output2"));
            path.put("input4_2", path.get("output3"));
            path.put("output4", args[1]);
        }
        Step1.run(path);
        Step2.run(path);
        Step3.run(path);
        Step4.run(path);
    }
}
