import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;


/**
 * Created by beyondwu on 2016/3/15.
 */
public class VectorMatrixMapper extends Mapper<LongWritable, Text, LongWritable, FloatWritable> {

    private Map<Long, Float> vector = new HashMap<Long, Float>();
    private FileSystem fs;

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        //super.cleanup(context);
        //fs.close();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //super.setup(context);
        URI[] cacheFiles = Job.getInstance(context.getConfiguration()).getCacheFiles();//context.getCacheFiles();
        System.out.println("cache files:"+context.getConfiguration().get("mapred.cache.files"));
        if (cacheFiles.length > 0) {
            Path path = new Path(cacheFiles[0].getPath());
            System.out.println(cacheFiles[0].toString() + "****");
            //fs = path.getFileSystem(context.getConfiguration());
            BufferedReader br = new BufferedReader(new FileReader(path.getName().toString()));
            String line;
            while ((line = br.readLine()) != null) {
                vector.put(Long.valueOf(line.split(",")[0]), Float.valueOf(line.split(",")[1]));
                System.out.println(line + "****");
            }
            //fs.close();
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] colValue = value.toString().split(",");
        System.out.println("cache files:"+context.getConfiguration().get("mapred.cache.files"));
        //System.out.println(value + "****" + colValue[0] + "****" + colValue[1]);
        if (colValue.length == 3) {
            try {
                if (vector.containsKey(Long.valueOf(colValue[1]))) {
                    context.write(new LongWritable(Long.valueOf(colValue[1])), new FloatWritable((Float.valueOf(colValue[2])) * vector.get(Long.valueOf(colValue[1]))));
                }
            } catch (Exception e) {
                System.out.println("****" + value + "****");
            }
        }
    }
}





