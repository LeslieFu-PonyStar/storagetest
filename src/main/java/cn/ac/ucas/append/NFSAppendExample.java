package cn.ac.ucas.append;


import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Random;

public class NFSAppendExample {
    private static String cities = "New York, Tokyo, London, Paris, Beijing, Sydney, Los Angeles, Shanghai, Dubai, Singapore, Hong Kong, Barcelona, Rome, Bangkok, Istanbul, San Francisco, Amsterdam, Toronto, Seoul, Chicago, Madrid, Vancouver, Taipei, Kuala Lumpur, Vienna, Washington D.C., Berlin, Moscow, Osaka, Melbourne, Mumbai, Miami, Rio de Janeiro, Sao Paulo, Houston, Boston, Dubai, Athens, Stockholm, Dublin, Copenhagen, Prague, Zurich, Helsinki, Brussels, Edinburgh, Lisbon, Auckland, Cape Town, Buenos Aires, Mexico City, Abu Dhabi, Krakow, Budapest, Kyoto, Oslo, Hamburg, Venice, Montreal, Warsaw, Geneva, Doha, Stuttgart, Lyon, Nice, Valencia, Frankfurt, Marseille, Dresden, Bruges, Salzburg, Innsbruck, Santorini, Granada, Cusco, Dubrovnik, Tallinn, Riga, Vilnius, Split, Reykjavik, Bergen, Zermatt, Bern, Lucerne, Interlaken, Hallstatt, Cesky Krumlov, Guilin, Lhasa.";
    private static int fileNum = 10000;
    private static int cityNum = 50;
    private static String nfsPath = "/Users/guoliangzhu/nfs-share/file.txt";
    public static void main(String[] args) {
        try {

            // 写入文件内容
            File file = new File(nfsPath);
            long fileSize = file.length();

            FileOutputStream fos = new FileOutputStream(nfsPath, true);
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fos, StandardCharsets.UTF_8));

            String[] cityList = cities.split(", ");
            Random random = new Random();

            long startTime = System.currentTimeMillis();


            for (int i = 0; i < fileNum; i++) {
                StringBuffer text = new StringBuffer("");
                for (int j = 0; j < random.nextInt(cityNum); j++) {
                    text.append(cityList[random.nextInt(cityList.length)]);
                    text.append("\n");
                }
                writer.write(text.toString());

                writer.close();
                fos.close();
                System.out.println("put success======="+i+"===========");


                fos = new FileOutputStream(nfsPath, true);
                writer = new BufferedWriter(new OutputStreamWriter(fos, StandardCharsets.UTF_8));
            }


            writer.close();
            fos.close();

            long endTime = System.currentTimeMillis();

            System.out.println("文件追加写完成。");
            System.out.println("原来文件大小：" + fileSize + " 字节");

            File updatedFile = new File(nfsPath);
            long updatedFileSize = updatedFile.length();
            System.out.println("更新后文件大小：" + updatedFileSize + " 字节");

            long elapsedTime = endTime - startTime;

            long ioOperations = fileNum;
            double iops = (double) ioOperations / (elapsedTime / 1000.0);
            double bandwidth = (double) (updatedFileSize-fileSize) / (elapsedTime / 1000.0);


            System.out.println("time:" + elapsedTime + " ms");

            System.out.println("IOPS:" + iops);

            System.out.println("bandwidth:" + bandwidth + " bytes/s");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

