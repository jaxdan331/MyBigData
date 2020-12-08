import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.util.Iterator;
import java.util.List;

public class WriteToHBase {
    public void createNamespace(String nsName) throws IOException {
        // 下面这四句话是每次操作数据库时都需要写的
        Configuration conf = HBaseConfiguration.create();
        conf.set("fs.defaultFS", "hdfs://hadoop-node1:9000");
        conf.set("hbase.zookeeper.quorum", "hadoop-node1,hadoop-node2,hadoop-node3");
        Connection conn = ConnectionFactory.createConnection(conf);

        // 我现在看明白了，不管是创建命名空间还是表，都得通过 admin
        Admin admin = conn.getAdmin();
        // 创建命名空间名为 nsName 的命名空间
        NamespaceDescriptor ns = NamespaceDescriptor.create(nsName).build();
        admin.createNamespace(ns);
        // 查看当前所有的命名空间（名）
        NamespaceDescriptor[] nss = admin.listNamespaceDescriptors();
        for (NamespaceDescriptor nsd : nss) {
            System.out.println(nsd);
        }
        admin.close();
    }

    public void createTable(String tableName, String[] families) throws Exception {
        // 下面这四句话是每次操作数据库时都需要写的
        Configuration conf = HBaseConfiguration.create();
        conf.set("fs.defaultFS", "hdfs://hadoop-node1:9000");
        conf.set("hbase.zookeeper.quorum", "hadoop-node1,hadoop-node2,hadoop-node3");
        Connection conn = ConnectionFactory.createConnection(conf);

        Admin admin = conn.getAdmin();
        // 先判断表是否已经存在
        TableName tb = TableName.valueOf(tableName);
        if (admin.tableExists(tb)) {
            // 如果存在就把它删掉
            System.out.println("table already exists!");
            // 先判断表是否处于使能状态，要删除一个表必须先禁用它
            if (admin.isTableEnabled(tb)) {
                admin.disableTable(tb);
            }
            admin.deleteTable(tb);
        }
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
        // 向表中添加列族
        for (int i = 0; i < families.length; i++) {
            table.addFamily(new HColumnDescriptor(families[i]));
        }
        admin.createTable(table);
        System.out.println("create table " + tableName + " ok");

        // 查看当前所有的表（名）
        TableName[] tns = admin.listTableNames();
        for (TableName tn : tns) {
            System.out.println(tn);
        }
        admin.close();
    }

    public void putData(String tableName, String[] families) throws Exception {
        // 创建表
        createTable(tableName, families);
        System.out.println("start insert data...");  // docker 镜像中显示不了中文，全用英文写

        // 下面这四句话是每次操作数据库时都需要写的
        Configuration conf = HBaseConfiguration.create();
        conf.set("fs.defaultFS", "hdfs://hadoop-node1:9000");
        conf.set("hbase.zookeeper.quorum", "hadoop-node1,hadoop-node2,hadoop-node3");
        Connection conn = ConnectionFactory.createConnection(conf);

        Table table = conn.getTable(TableName.valueOf(tableName));
        File file = new File("./input/input-test.txt");

        long start = System.currentTimeMillis();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            int i = 0;
            String line;
            // 下面这个循环必须这样写，先把 reader.readLine() 的值赋给 line 否则打印出来是隔行的！！
            while ((line = reader.readLine()) != null) {
                i++;
                String[] items = line.split(" ");
                // 新建一行
                Put row_i = new Put(Bytes.toBytes("row" + i));
                // 向行中插入用户id、电影id、评分、时间戳
                row_i.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("usrId"), Bytes.toBytes(items[0]));
                row_i.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("mvId"), Bytes.toBytes(items[1]));
                row_i.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("rate"), Bytes.toBytes(items[2]));
                row_i.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("time"), Bytes.toBytes(items[3]));
                table.put(row_i);
            }
            reader.close();
            System.out.println("Totally inserted " + i + "rows.");
        } catch (IOException e) {
            e.printStackTrace();
        }
        table.close();
        long stop = System.currentTimeMillis();
        System.out.println("finished in " + (stop - start) * 1.0 / 1000 + " s.");
    }

//    public void printData(String tableName) throws IOException {
//        // 下面这四句话是每次操作数据库时都需要写的
//        Configuration conf = HBaseConfiguration.create();
//        conf.set("fs.defaultFS", "hdfs://hadoop-node1:9000");
//        conf.set("hbase.zookeeper.quorum", "hadoop-node1,hadoop-node2,hadoop-node3");
//        Connection conn = ConnectionFactory.createConnection(conf);
//
//        Table table = conn.getTable(TableName.valueOf(tableName));
//        Scan scan = new Scan();
//        ResultScanner rs = table.getScanner(scan);
//        Iterator<Result> it = rs.iterator();
//        int i = 0;
//        while (it.hasNext()) {
//            i++;
//        }
//        System.out.println("There are " + i + " rows.");
//        table.close();
//    }

    public void readFileTest() throws IOException {
        File file = new File("./input/input-test.txt");
        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            int i = 0;
            String line;
            // 下面这个循环必须这样写，先把 reader.readLine() 的值赋给 line 否则打印出来是隔行的！！
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
                i++;
            }
            reader.close();
            System.out.println("Totally " + i + "lines.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        WriteToHBase wtb = new WriteToHBase();
        // wtb.createNamespace("movie");
        // 注意写表名的时候一定要把命名空间的前缀加上，要不然默认是把表放到 default 的命名空间中去了
        wtb.putData("movie:movieLens", new String[] { "cf1", "cf2" });
        // wtb.printData("movie:movieLens");

        // wtb.readFileTest();
    }
}

