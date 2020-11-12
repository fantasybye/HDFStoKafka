package Utils;

import Bean.UserBehavior;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

public class UserBehaviorCsvFileReader implements Supplier<UserBehavior> {

    private final String filePath;
    private final BufferedReader br;

    public UserBehaviorCsvFileReader(String filePath) throws IOException {

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

    @Override
    public UserBehavior get() {
        UserBehavior userBehavior = null;
        try{
//            if(csvReader.readRecord()) {
//                csvReader.getRawRecord();
//                userBehavior = new UserBehavior(
//                        Long.valueOf(csvReader.get(0)),
//                        Long.valueOf(csvReader.get(1)),
//                        Long.valueOf(csvReader.get(2)),
//                        csvReader.get(3),
//                        new Date(Long.valueOf(csvReader.get(4))*1000L));
//            }
            String str;
            if((str = br.readLine())!=null){
                String[] split = str.split(",");
                userBehavior = new UserBehavior(
                        Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        Long.parseLong(split[2]),
                        split[3],
                        new Date(Long.parseLong(split[4])*1000L));
            }
        } catch (IOException e) {
            throw new NoSuchElementException("IOException from " + filePath);
        }

        if (null==userBehavior) {
            throw new NoSuchElementException("All records read from " + filePath);
        }

        return userBehavior;
    }

    private FileSystem getFiledSystem() throws IOException {
        Configuration configuration = new Configuration();
        configuration.addResource(new Path("../../../core-site.xml"));
        configuration.addResource(new Path("../../../hdfs-site.xml"));
        return FileSystem.get(configuration);
    }
}
