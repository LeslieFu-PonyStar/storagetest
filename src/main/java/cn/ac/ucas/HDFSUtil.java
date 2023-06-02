package cn.ac.ucas;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSUtil {
    private FileSystem fs;
    /**
     * 连接到hdfs服务器
     * @param uri 用来连接hdfs的uri，形如"hdfs://<hdfs ip addr>:<hdfs port>"
     * @param username 用来登录hdfs的用户名
     */
    public HDFSUtil(String uri, String username){
        try {
            this.fs = FileSystem.get(new URI(uri), new Configuration(), username);
        } catch (IOException | InterruptedException | URISyntaxException e) {
            e.printStackTrace();
        }
    }
    /** 
     * 检测是否能访问到hdfs的目标文件路径，请注意访问的用户是否有目标文件的权限
     * @return
     */
    public boolean isConnected(String path) throws IllegalArgumentException, IOException{
        return fs.exists(new Path(path));
    }
    /**
     * 将输入流写入hdfs中，本函数支持追加写
     * @param inputStream 要写入的数据的输入流
     * @param hdfsPath 文件的hdfs路径
     * @param isOverwrite 指是否为覆盖写，如果该值为false而对应文件在hdfs中不存在，那么会新建,如果存在则会追加写
     * @param bufferSize 写文件时的buffersize，为大于零的整数，小文件可以随意选，大文件可以根据自己的需求
     * @throws IOException
     * @throws InterruptedException
     */
    public void writeFile(InputStream inputStream, String hdfsPath, boolean isOverwrite, int bufferSize) throws IOException, InterruptedException {
        Path outputPath = new Path(hdfsPath);
        OutputStream outputStream = null;
        if(isOverwrite == true){
            outputStream = this.fs.create(outputPath);
        }else{
            if(!this.fs.exists(outputPath)){
                System.out.println(hdfsPath + " is not exist. A new file will be created.\n");
                outputStream = this.fs.create(outputPath);
            }
            outputStream = fs.append(outputPath);
        }
        byte[] buffer = new byte[bufferSize];
        int bytesRead = inputStream.read(buffer);
        while (bytesRead > 0) {
            outputStream.write(buffer, 0, bytesRead);
            bytesRead = inputStream.read(buffer);
        }
    }
    /**
     * 将本地文件上传至hdfs
     * @param localFilePath 本地文件路径
     * @param hdfsPath 远端文件路径
     * @throws IOException
     */
    public void uploadFileToHDFS(String localFilePath, String hdfsPath) throws IOException {
        Path src = new Path(localFilePath);
        Path dst = new Path(hdfsPath);
        this.fs.copyFromLocalFile(src,dst);
    }
    /**
     * 这是从hdfs中读取文本文件的函数
     * @param hdfsPath hdfs路径
     * @param desired 需要从文件的哪一个字节开始读
     * @return 目标文件的输入流
     */
    public FSDataInputStream readTextFile(String hdfsPath, long desired){
        try {
            Path path = new Path(hdfsPath);
            if(!this.fs.exists(path)){
                System.out.println(hdfsPath + " is not exist.\n");
                return null;
            }
            FSDataInputStream inputStream = this.fs.open(path);
            // 后期修改
            inputStream.seek(desired);
            return inputStream;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        
    }

    public void close(){
        try {
            this.fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
        String[] ss = {"1", "2", "3"};
        for (String s : ss){
            System.out.println(s);
        }
    }
}
