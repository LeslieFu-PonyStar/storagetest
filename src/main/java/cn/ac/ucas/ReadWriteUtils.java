package cn.ac.ucas;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * 读写工具类，里面都是静态方法
 */
public class ReadWriteUtils {
    /**
     * 该函数用来将字节流新写入文件中
     * 
     * @param data       输出字节流
     * @param fileName   新文件
     * @param isAppend   是否追加写
     * @param bufferSize 缓冲区大小，推荐8192
     * @throws IOException
     */
    public static void writeFile(InputStream inputStream, String fileName, boolean isAppend, int bufferSize)
            throws IOException {
        FileOutputStream outputStream = new FileOutputStream(fileName, isAppend);
        byte[] buffer = new byte[bufferSize];
        int bytesRead;
        while ((bytesRead = inputStream.read(buffer)) != -1) {
            outputStream.write(buffer, 0, bytesRead);
        }
        outputStream.close();
    }

    public static InputStream readFile(String filePath) throws FileNotFoundException {
        InputStream inputStream = new FileInputStream(new File(filePath));
        return inputStream;
    }

    /**
     * 该函数用来检验目标文件夹是否正确挂载，是否能访问nfs服务器
     * 
     * @param nfsPath 目标挂载文件夹
     * @return true & false
     */
    public static boolean checkNFSMount(String nfsPath) {
        Path targetPath = Paths.get(nfsPath);
        FileStore fileStore;
        try {
            fileStore = Files.getFileStore(targetPath);
            String fsType = fileStore.type();
            if (fsType.contains("nfs")) {
                return true;
            } else {
                System.out.println("Target directory not mounted.");
                return false;
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

    }

    /**
     * 一个将输入流中内容输出到控制台的函数
     * 
     * @param inputStream 输入流
     * @throws IOException
     */
    public static void printInputStream(InputStream inputStream) throws IOException {
        try {
            byte[] buffer = new byte[8192];
            int length = 0;
            while ((length = inputStream.read(buffer)) > 0) {
                System.out.write(buffer, 0, length);
            }
        } finally {
            inputStream.close();
        }
    }

    /**
     * 将字符串转换为输入流的函数
     * 
     * @param str
     * @return
     */
    public static InputStream stringToInputStream(String str) {
        byte[] bytes = str.getBytes();
        InputStream inputStream = new ByteArrayInputStream(bytes);
        return inputStream;
    }
}
