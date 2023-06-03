package cn.ac.ucas;

import java.io.IOException;
import java.io.InputStream;

import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.AppendObjectRequest;
import com.obs.services.model.DownloadFileRequest;
import com.obs.services.model.ObsObject;
import com.obs.services.model.UploadFileRequest;

public class ObsUtil {
    private ObsClient obsClient;

    public ObsUtil(String endPoint, String ak, String sk) {
        this.obsClient = new ObsClient(ak, sk, endPoint);
    }

    /**
     * 请注意，如果后续有追加写的需求，在初次创建文件时需要选择isAppendable，否则会导致后续无法进行追加写，这是华为云对象存储的限制
     * 
     * @param bucketName   桶名
     * @param obsKey       数据的键，可以以“/”分隔，就像文件系统那样
     * @param inputStream  要写入的输入流
     * @param isAppendable 文件后续是否追加写
     */

    public void putObject(String bucketName, String obsKey, InputStream inputStream, boolean isAppendable) {
        if (!isAppendable)
            obsClient.putObject(bucketName, obsKey, inputStream);
        else {
            AppendObjectRequest appendObjectRequest = new AppendObjectRequest(bucketName);
            appendObjectRequest.setObjectKey(obsKey);
            appendObjectRequest.setInput(inputStream);
            this.obsClient.appendObject(appendObjectRequest);
        }
    }

    /**
     * 读取对应obsKey的内容
     * 
     * @param bucketName
     * @param obsKey
     * @return
     */
    public InputStream readObject(String bucketName, String obsKey) throws ObsException{
        ObsObject obsObject = this.obsClient.getObject(bucketName, obsKey);
        return obsObject.getObjectContent();
    }

    /**
     * 如果是追加写，必须在一开始就选择可追加写选项！注意！如果对应的key不存在，那么会在对应key的位置新建对象，并且是可追加的
     * 
     * @param bucketName
     * @param obsKey
     * @param inputStream
     */
    public void appendObject(String bucketName, String obsKey, InputStream inputStream) {
        ObsObject obsObject = null;
        try{
            obsObject = this.obsClient.getObject(bucketName, obsKey);
        }catch(ObsException e){
            String errString = e.getErrorCode();
            if(errString.equals("NoSuchKey")){
                putObject(bucketName, obsKey, inputStream, true);
            }else{
                System.out.println(e.getErrorMessage());
            }
            return;
        }
        // 获取文件尾部偏移，这里sdk实现的函数很奇怪
        long tail = obsObject.getMetadata().getContentLength();
        AppendObjectRequest appendObjectRequest = new AppendObjectRequest(bucketName);
        appendObjectRequest.setObjectKey(obsKey);
        appendObjectRequest.setInput(inputStream);
        appendObjectRequest.setPosition(tail);
        this.obsClient.appendObject(appendObjectRequest);
    }
    /**
     * 上传文件本地文件到对象存储服务桶中的代码
     * @param bucketName
     * @param obsKey
     * @param filePath
     */
    public void uploadFile(String bucketName, String obsKey, String filePath) {
        UploadFileRequest uploadFileRequest = new UploadFileRequest(bucketName, obsKey);
        uploadFileRequest.setUploadFile(filePath);
        this.obsClient.uploadFile(uploadFileRequest);
    }
    // upload支持分段断续上传(一般是很大的文件)其中partSize是分段大小，taskNum是分段上传任务并行数
    public void uploadFile(String bucketName, String obsKey, String filePath, long partSize, int taskNum) {
        UploadFileRequest uploadFileRequest = new UploadFileRequest(bucketName, obsKey, filePath, partSize, taskNum);
        this.obsClient.uploadFile(uploadFileRequest);
    }
    /**
     * 下载文件到本地
     * @param bucketName
     * @param obsKey
     * @param filePath
     */
    public void downloadFile(String bucketName, String obsKey, String filePath) {
        DownloadFileRequest downloadFileRequest = new DownloadFileRequest(bucketName, obsKey);
        downloadFileRequest.setDownloadFile(filePath);
        this.obsClient.downloadFile(downloadFileRequest);
    }

    // appendFile方法一直有问题，感觉是华为官方还没开发好
    // public void appendFile(String bucketName, String obsKey, String filePath){
    //     ReadFileResult readFileResult = null;
    //     WriteFileRequest writeFileRequest = new WriteFileRequest(bucketName, obsKey, new File(filePath));
    //     try{
    //         readFileResult = this.obsClient.readFile(new ReadFileRequest(bucketName, obsKey));
    //     }catch(ObsException e){
    //         String errString = e.getErrorCode();
    //         if(errString.equals("NoSuchKey")){
    //             writeFileRequest.setPosition(0);
    //             this.obsClient.appendFile(writeFileRequest);
    //         }else{
    //             System.out.println(e.getErrorMessage());
    //         }
    //         return;
    //     }
    //     long tail = readFileResult.getMetadata().getContentLength();
    //     writeFileRequest.setPosition(tail);
    //     this.obsClient.appendFile(writeFileRequest);
    // }

    public void close() throws IOException {
        obsClient.close();
    }
}
