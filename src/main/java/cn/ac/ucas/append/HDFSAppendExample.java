package cn.ac.ucas.append;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Random;

public class HDFSAppendExample {
    private static String cities = "New York, Tokyo, London, Paris, Beijing, Sydney, Los Angeles, Shanghai, Dubai, Singapore, Hong Kong, Barcelona, Rome, Bangkok, Istanbul, San Francisco, Amsterdam, Toronto, Seoul, Chicago, Madrid, Vancouver, Taipei, Kuala Lumpur, Vienna, Washington D.C., Berlin, Moscow, Osaka, Melbourne, Mumbai, Miami, Rio de Janeiro, Sao Paulo, Houston, Boston, Dubai, Athens, Stockholm, Dublin, Copenhagen, Prague, Zurich, Helsinki, Brussels, Edinburgh, Lisbon, Auckland, Cape Town, Buenos Aires, Mexico City, Abu Dhabi, Krakow, Budapest, Kyoto, Oslo, Hamburg, Venice, Montreal, Warsaw, Geneva, Doha, Stuttgart, Lyon, Nice, Valencia, Frankfurt, Marseille, Dresden, Bruges, Salzburg, Innsbruck, Santorini, Granada, Cusco, Dubrovnik, Tallinn, Riga, Vilnius, Split, Reykjavik, Bergen, Zermatt, Bern, Lucerne, Interlaken, Hallstatt, Cesky Krumlov, Guilin, Lhasa.";
    private static int fileNum = 1000;
    private static int cityNum = 10;

    public static void main(String[] args) throws InterruptedException, URISyntaxException {
//        String hdfsUri = "hdfs://47.111.132.175:9000";
        String hdfsUri = "hdfs://192.168.8.11:9000";

        String filePath = "/fhw/hdfsfile";

        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(new URI(hdfsUri), conf, "root");
            FileStatus fileStatus = fs.getFileStatus(new Path(filePath));
            long fileSize = fileStatus.getLen();


            FSDataInputStream inputStream = fs.open(new Path(filePath));
            long currentLength = fs.getFileStatus(new Path(filePath)).getLen();
            inputStream.seek(currentLength);
            FSDataOutputStream outputStream = fs.append(new Path(filePath));


//            String data = "This is new data to append.";

            String[] cityList = cities.split(", ");
            Random random = new Random();

            long startTime = System.currentTimeMillis();


            for (int i = 0; i < fileNum; i++) {
                StringBuffer text = new StringBuffer("");
                for (int j = 0; j < random.nextInt(cityNum); j++) {
                    text.append(cityList[random.nextInt(cityList.length)]);
                    text.append("\n");
                }
                outputStream.writeBytes(text.toString());
                outputStream.close();
                inputStream.close();
                System.out.println("put success======="+i+"===========");
                inputStream = fs.open(new Path(filePath));
                currentLength = fs.getFileStatus(new Path(filePath)).getLen();
                inputStream.seek(currentLength);
                outputStream = fs.append(new Path(filePath));
            }

            outputStream.close();
            inputStream.close();
            System.out.println("data has append to file.");

            FileStatus updatedStatus = fs.getFileStatus(new Path(filePath));
            long updatedSize = updatedStatus.getLen();

            System.out.println("file size is  " + fileSize + " bytes");
            System.out.println("updated file size is  " + updatedSize + " bytes");
            System.out.println("append size is  " + (updatedSize-fileSize) + " bytes");



            long endTime = System.currentTimeMillis();
            long elapsedTime = endTime - startTime;

            long ioOperations = fileNum;
            double iops = (double) ioOperations / (elapsedTime / 1000.0);
            double bandwidth = (double) (updatedSize-fileSize) / (elapsedTime / 1000.0);

            System.out.println("time:" + elapsedTime + " ms");
            System.out.println("IOPS:" + iops);
            System.out.println("bandwidth:" + bandwidth + " byte/s");


        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
