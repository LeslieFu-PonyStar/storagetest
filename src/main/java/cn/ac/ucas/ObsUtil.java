package cn.ac.ucas;

import java.io.IOException;
import java.io.InputStream;

import com.obs.services.ObsClient;
import com.obs.services.model.AppendObjectRequest;
import com.obs.services.model.ObsObject;

public class ObsUtil {
    private ObsClient obsClient;
    public ObsUtil(String endPoint, String ak, String sk){
        this.obsClient = new ObsClient(ak, sk, endPoint);
    }
    /**
     * 请注意，如果后续有追加写的需求，在初次创建文件时需要选择isAppendable，否则会导致后续无法进行追加写
     * @param bucketName 桶名
     * @param obsKey 数据的键，可以以“/”分隔，就像文件系统那样
     * @param inputStream 要写入的输入流
     * @param isAppendable 文件后续是否追加写
     */
    public void putObject(String bucketName, String obsKey, InputStream inputStream, boolean isAppendable){
        if(!isAppendable)
            obsClient.putObject(bucketName, obsKey, inputStream);
        else{
            AppendObjectRequest appendObjectRequest = new AppendObjectRequest(bucketName);
            appendObjectRequest.setObjectKey(obsKey);
            appendObjectRequest.setInput(inputStream);
            this.obsClient.appendObject(appendObjectRequest);
        }
    }

    public InputStream readObject(String bucketName, String obsKey){
        ObsObject obsObject = this.obsClient.getObject(bucketName, obsKey);
        return obsObject.getObjectContent();
    }
    /**
     * 如果是追加写，必须在一开始就选择可追加写选项！注意！
     * @param bucketName
     * @param obsKey
     * @param inputStream
     */
    public void appendObject(String bucketName, String obsKey, InputStream inputStream){
        ObsObject obsObject = this.obsClient.getObject(bucketName, obsKey);
        // 获取文件尾部偏移，这里sdk实现的函数很奇怪
        long tail = obsObject.getMetadata().getContentLength();
        AppendObjectRequest appendObjectRequest = new AppendObjectRequest(bucketName);
        appendObjectRequest.setObjectKey(obsKey);
        appendObjectRequest.setInput(inputStream);
        appendObjectRequest.setPosition(tail);
        obsClient.appendObject(appendObjectRequest);
    }
    public void close() throws IOException{
        obsClient.close();
    }
}
