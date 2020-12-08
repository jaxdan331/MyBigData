import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.util.Iterator;
import java.util.List;

public class PrintHBase {
    public static void printData(String tableName) throws IOException {
        // 下面这四句话是每次操作数据库时都需要写的
        Configuration conf = HBaseConfiguration.create();
        conf.set("fs.defaultFS", "hdfs://hadoop-node1:9000");
        conf.set("hbase.zookeeper.quorum", "hadoop-node1,hadoop-node2,hadoop-node3");
        Connection conn = ConnectionFactory.createConnection(conf);

        Table table = conn.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner rs = table.getScanner(scan);
        Iterator<Result> it = rs.iterator();
        int i = 0;
        while (it.hasNext()) {
            Result next = it.next();  // 这个读取的是一行的数据
            List<Cell> cells = next.listCells();
            String usrId = "";
            String mvId = "";
            String rate = "";
            // 遍历改行每一个 cell
            for (Cell cell : cells) {
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));  // 获取该 cell 所在的列
                String value = Bytes.toString(CellUtil.cloneValue(cell));  // 获取该 cell 的值
                if (qualifier.equals("usrId")) {
                    usrId = value;
                }else if (qualifier.equals("mvId")) {
                    mvId = value;
                }else if (qualifier.equals("rate")) {
                    rate = value;
                }
            }
            String line = usrId + " " + mvId + " " + rate;
            System.out.println(line);
            i++;
        }
        System.out.println("There are " + i + " rows.");
        table.close();
    }

    public static void main(String[] args) throws IOException {
        PrintHBase.printData("movie:movieLens");
    }
}
