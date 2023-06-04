package cn.ac.ucas.raw;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils; 

/**
 * @author Zhao Ruilin 202228015059004
 * @version 0.1
 */

public class HadoopTest {
    private static String cities = "New York, Tokyo, London, Paris, Beijing, Sydney, Los Angeles, Shanghai, Dubai, Singapore, Hong Kong, Barcelona, Rome, Bangkok, Istanbul, San Francisco, Amsterdam, Toronto, Seoul, Chicago, Madrid, Vancouver, Taipei, Kuala Lumpur, Vienna, Washington D.C., Berlin, Moscow, Osaka, Melbourne, Mumbai, Miami, Rio de Janeiro, Sao Paulo, Houston, Boston, Dubai, Athens, Stockholm, Dublin, Copenhagen, Prague, Zurich, Helsinki, Brussels, Edinburgh, Lisbon, Auckland, Cape Town, Buenos Aires, Mexico City, Abu Dhabi, Krakow, Budapest, Kyoto, Oslo, Hamburg, Venice, Montreal, Warsaw, Geneva, Doha, Stuttgart, Lyon, Nice, Valencia, Frankfurt, Marseille, Dresden, Bruges, Salzburg, Innsbruck, Santorini, Granada, Cusco, Dubrovnik, Tallinn, Riga, Vilnius, Split, Reykjavik, Bergen, Zermatt, Bern, Lucerne, Interlaken, Hallstatt, Cesky Krumlov, Guilin, Lhasa.";
    private static int fileNum = 1000;
    private static int cityNum = 10;
    private static String RedisAddress = "127.0.0.1";
    private static int MaxNum = 999999;
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException{
        //Create a random file
        String[] cityList = cities.split(", ");
        Random random = new Random();
        
        OutputStream f = new FileOutputStream("testfile.txt");
        String city;
        int num;
        
        //First create a file
        for (int i = 0; i < fileNum; i++) {
            for (int j = 0; j < cityNum; j++){
                city = cityList[random.nextInt(cityList.length)];
                num = random.nextInt(MaxNum);
                f.write((city+":"+Integer.toString(num)+"\n").getBytes());
            }
        }
        
        //Connect to hdfs and put it inside
        String file = "hdfs://192.168.8.11:9000";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(file), conf, "root");
        Path localPath = new Path("testfile.txt");
        Path hdfsPath = new Path("/testfile.txt");
        
        //Test the speed
        long start = System.currentTimeMillis();
        fs.copyFromLocalFile(localPath, hdfsPath);
        System.out.println(System.currentTimeMillis() - start);
        System.out.println("Copying operation completed.\n");
        
        fs.close();
    }
}
