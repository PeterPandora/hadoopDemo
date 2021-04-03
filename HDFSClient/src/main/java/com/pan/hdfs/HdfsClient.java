package com.pan.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * @author 潘聪
 * @description
 * @date 2021/3/21 10:37
 */
public class HdfsClient {

    private FileSystem fileSystem;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        URI uri = new URI("hdfs://hadoop102:8020");
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        String user = "pandora";
        fileSystem = FileSystem.get(uri, configuration, user);
    }

    @After
    public void close() throws IOException {
        fileSystem.close();
    }

    // 创建目录
    @Test
    public void testmkdir() throws URISyntaxException, IOException, InterruptedException {

        fileSystem.mkdirs(new Path("/xiyou/huaguoshan2"));

    }

    // 上传
    @Test
    public void testPut() throws IOException {
        fileSystem.copyFromLocalFile(false, true, new Path("src/main/resources/log4j.properties"), new Path("hdfs://hadoop102/xiyou/huaguoshan"));
    }

    // 下载
    @Test
    public void testGet() throws IOException {
        fileSystem.copyToLocalFile(false, new Path("/xiyou/huaguoshan/"), new Path("src/main/resources/"), false);
    }

    // 删除
    @Test
    public void testDel() throws IOException {
        fileSystem.delete(new Path("/xiyou/huaguoshan/"), true);
    }

    // 获取文件详细信息
    @Test
    public void fileDetail() throws IOException {
        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println(fileStatus.getPath());
            System.out.println(fileStatus.getPermission());

            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }
    }

    // 判断是文件还是目录
    @Test
    public void testFile() throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isFile()) {
                System.out.println("文件：" + fileStatus.getPath().getName());
            } else {
                System.out.println("目录：" + fileStatus.getPath().getName());
            }
        }
    }
}
