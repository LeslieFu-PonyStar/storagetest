package cn.ac.ucas.append;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ha.HAAdmin;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.*;

import java.io.IOException;

public class HBaseAppendExample {
    private static HTable table;
    private static HBaseAdmin hAdmin;

    static String tableName = "city";
    static String rowKey = "rowkey_1";
    static String columnFamily = "city_info";
    static String columnQualifier = "test1";
    static int num_appends = 1000;


    private static String cities = "New York, Tokyo, London, Paris, Beijing, Sydney, Los Angeles, Shanghai, Dubai, Singapore, Hong Kong, Barcelona, Rome, Bangkok, Istanbul, San Francisco, Amsterdam, Toronto, Seoul, Chicago, Madrid, Vancouver, Taipei, Kuala Lumpur, Vienna, Washington D.C., Berlin, Moscow, Osaka, Melbourne, Mumbai, Miami, Rio de Janeiro, Sao Paulo, Houston, Boston, Dubai, Athens, Stockholm, Dublin, Copenhagen, Prague, Zurich, Helsinki, Brussels, Edinburgh, Lisbon, Auckland, Cape Town, Buenos Aires, Mexico City, Abu Dhabi, Krakow, Budapest, Kyoto, Oslo, Hamburg, Venice, Montreal, Warsaw, Geneva, Doha, Stuttgart, Lyon, Nice, Valencia, Frankfurt, Marseille, Dresden, Bruges, Salzburg, Innsbruck, Santorini, Granada, Cusco, Dubrovnik, Tallinn, Riga, Vilnius, Split, Reykjavik, Bergen, Zermatt, Bern, Lucerne, Interlaken, Hallstatt, Cesky Krumlov, Guilin, Lhasa.";
    private static int cityNum = 100;

    public static void main(String[] args) throws IOException {

//        writeTest();
        createTable();

        writeTest();

        long startTime = System.currentTimeMillis();
        long byteSize = appendToHbase();
        long endTime = System.currentTimeMillis();
        long blockSize = getBlockSize();

        System.out.println("data has append to file.");
        System.out.println("append byteSize is: "+byteSize+"bytes");
        //always be 65536?????
        System.out.println("columnFamily---BlockSize is: "+ blockSize);

        table.close();
        hAdmin.close();


        long elapsedTime = endTime - startTime;
        long ioOperations = num_appends;
        double iops = (double) ioOperations / (elapsedTime / 1000.0);
        double bandwidth = (double) byteSize / (elapsedTime / 1000.0);

        System.out.println("time:" + elapsedTime + " ms");
        System.out.println("IOPS:" + iops);
        System.out.println("bandwidth:" + bandwidth + " byte/s");

    }


    public static void createTable() throws IOException{
//        String tableName= "city";
        // create table descriptor
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
        // create column descriptor
        HColumnDescriptor cf = new HColumnDescriptor(columnFamily);
        htd.addFamily(cf);
        // configure HBase
        Configuration configuration = HBaseConfiguration.create();
       configuration.set("hbase.zookeeper.quorum", "myhbase");
       configuration.set("hbase.zookeeper.property.clientPort", "2181");


//        HBaseAdmin hAdmin = new HBaseAdmin(configuration);
        hAdmin = new HBaseAdmin(configuration);

        if (hAdmin.tableExists(tableName)) {
            System.out.println("Table already exists");
            hAdmin.disableTable(TableName.valueOf(tableName));
            hAdmin.deleteTable(TableName.valueOf(tableName));
            System.out.println("Table has been deleted");
        }
        hAdmin.createTable(htd);
        System.out.println("table "+tableName+ " created successfully");
//        hAdmin.close();

        table = new HTable(configuration,tableName);
    }

    public static long getBlockSize() throws IOException {
        TableName tableName_1 = TableName.valueOf(tableName);
        long blockSize = 0;
        if (hAdmin.tableExists(tableName_1)) {
            HTableDescriptor tableDescriptor = hAdmin.getTableDescriptor(tableName_1);
            HColumnDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();

            System.out.println("Column Families:");
            for (HColumnDescriptor columnFamily : columnFamilies) {
                System.out.println("  Name: " + Bytes.toString(columnFamily.getName()));
                System.out.println("  Block Size: " + columnFamily.getBlocksize());
                blockSize = columnFamily.getBlocksize();
            }
        } else {
            System.out.println("Table " + tableName + " does not exist.");
        }
        return blockSize;
    }



    public static void writeTest() throws IOException {
        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(columnQualifier), Bytes.toBytes("hello world"));
        table.put(put);
    }



    public static long appendToHbase() throws IOException {
        long fileSize_temp = 0;
        long retFileSize = 0;
        for (int i = 0; i < num_appends; i++){
            String text = getTestData();

            fileSize_temp = text.getBytes().length;
            retFileSize += fileSize_temp;

            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            byte[] oldValue = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(columnQualifier));

            Put put = new Put(Bytes.toBytes(rowKey));
            if(oldValue != null)
                put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(columnQualifier), Bytes.add(oldValue, Bytes.toBytes(text)));
            else
                put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(columnQualifier), Bytes.toBytes(text));

            table.put(put);

            System.out.println("put success====="+i+"========");
        }
        return retFileSize;
    }

    public static String getTestData(){
        String[] cityList = cities.split(", ");
        Random random = new Random();

        StringBuffer text = new StringBuffer("");
        for (int j = 0; j < random.nextInt(cityNum); j++) {
            text.append(cityList[random.nextInt(cityList.length)]);
            text.append("\n");
        }
//        System.out.println(text);
        return text.toString();
    }




}

