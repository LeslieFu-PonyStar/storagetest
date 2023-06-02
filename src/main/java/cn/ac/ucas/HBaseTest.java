package cn.ac.ucas;

import java.io.IOException;

public class HBaseTest {
    public static void main(String[] args) throws IOException {
        HBaseUtil hBaseUtil = new HBaseUtil("myhbase", "2181");
        hBaseUtil.putData("test_table", "cn.ac.ucas", "cf1", "name", "lesliefu");
    }
}
