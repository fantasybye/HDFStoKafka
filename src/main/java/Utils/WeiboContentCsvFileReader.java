package Utils;

import Bean.WeiboContent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.function.Supplier;

public class WeiboContentCsvFileReader implements Supplier<WeiboContent> {

    private final String filePath;
    private final BufferedReader br;
    public WeiboContentCsvFileReader(String filePath) throws IOException {

        this.filePath = filePath;
        try {
            Path path = new Path(filePath);
            //    private CsvReader csvReader;
            FSDataInputStream fsDataInputStream = this.getFiledSystem().open(path);
            br = new BufferedReader(new InputStreamReader(fsDataInputStream));
        } catch (IOException e) {
            throw new IOException("Error reading TaxiRecords from file: " + filePath, e);
        }
    }


    private FileSystem getFiledSystem() throws IOException {
        Configuration configuration = new Configuration();
        configuration.addResource(new Path("core-site.xml"));
        configuration.addResource(new Path("hdfs-site.xml"));
        return FileSystem.get(configuration);
    }

    @Override
    public WeiboContent get() {
        WeiboContent weiboContent = null;
        try {
            br.readLine();
            String str;
            if((str = br.readLine())!=null){
                String[] split = str.split(",");
                System.out.println(str);
                weiboContent = new WeiboContent(
                        split[0],
                        Long.parseLong(split[1]),
                        split[2],
                        new Date(Long.parseLong(split[3])*1000L)
                );
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return weiboContent;
    }
}
