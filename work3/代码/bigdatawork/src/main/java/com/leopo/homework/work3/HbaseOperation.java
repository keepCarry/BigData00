package com.leopo.homework.work3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseOperation {

    public static Configuration conf;
    public static Connection conn;

    /**
     * 初始化连接
     *
     * @param zkClientPort
     * @param zkQuorum
     * @param master
     */
    public static void init(String zkClientPort, String zkQuorum, String master) {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", zkClientPort);
        conf.set("hbase.zookeeper.quorum", zkQuorum);
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            System.out.println("create connection exception");
            e.printStackTrace();
        }
        System.out.println("create connection success");
    }

    /**
     * 关闭连接
     */
    public static void closeClient() {
        if (conn != null) {
            try {
                conn.close();
            } catch (Throwable t) {
                System.out.println("close connection failed");
                t.printStackTrace();
            }
            System.out.println("close connection success");
        }
    }


    /**
     * 插入数据
     *
     * @param tableName
     * @param rowKey
     * @param columnFamilys
     * @param columns
     * @param values
     * @throws IOException
     */
    public static void insertData(String tableName, String rowKey, String columnFamilys, String[] columns,
                                  String[] values) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        for (int i = 0; i < columns.length; i++) {
            put.addColumn(Bytes.toBytes(columnFamilys), Bytes.toBytes(columns[i]), Bytes.toBytes(values[i]));
        }
        table.put(put);
        System.out.println("data inserted success");
        table.close();
    }

    /**
     * 删除数据
     *
     * @param tableName
     * @param rowkey
     * @throws IOException
     */
    public static void deleteRow(String tableName, String rowkey) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        table.delete(delete);
        table.close();
        System.out.println("row" + rowkey + " is deleted");
    }

    /**
     * 查询数据
     */
    public static void query(String tableName, String rowkey) throws IOException {
        Get get = new Get(Bytes.toBytes(rowkey));
        Table table = conn.getTable(TableName.valueOf(tableName));
        Result result = table.get(get);
        if (result == null || result.listCells() == null) {
            System.out.println("cannot find the result");
            return;
        }

        for (Cell cell : result.listCells()) {
            String qualifier = new String(CellUtil.cloneQualifier(cell));
            String value = new String(CellUtil.cloneValue(cell), "UTF-8");
            System.out.println(qualifier + " : " + value);
        }
    }


    public static void main(String[] args) throws Exception {
        init("2181", "jikehadoop02", "");
        insertData("liupeng:student", "G20200388010284", "name", new String[]{"name"}, new String[]{"刘澎"});
        insertData("liupeng:student", "G20200388010284", "info", new String[]{"student_id", "class"}, new String[]{"G20200388010284", "2"});
        insertData("liupeng:student", "G20200388010284", "score", new String[]{"understanding", "programming"}, new String[]{"80", "80"});
        query("liupeng:student", "G20200388010284");
        deleteRow("liupeng:student", "G20200388010284");
        query("liupeng:student", "G20200388010284");
        closeClient();
    }
}
