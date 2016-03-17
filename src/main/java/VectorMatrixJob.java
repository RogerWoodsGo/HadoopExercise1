import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by beyondwu on 2016/3/15.
 */
public class VectorMatrixJob extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        if(strings.length < 2){
            System.out.println("please add input and output path");
            System.exit(-1);
        }
        String cachePath = conf.get("CachePath");
        Job job = Job.getInstance(conf);
        job.setJobName("matrix-dot-product");
        job.setJarByClass(VectorMatrixJob.class);
        job.addCacheFile(new Path(cachePath).toUri());
        System.out.println("++++++++++" + cachePath.toString());

        job.setOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(FloatWritable.class);
        job.setOutputValueClass(FloatWritable.class);

        job.setMapperClass(VectorMatrixMapper.class);
        job.setReducerClass(VectorMatrixReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(strings[0]));
        System.out.println("++++++++++" + strings[0]);
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));
        return (job.waitForCompletion(true) ? 0 : 1);
    }
    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new Configuration(), new VectorMatrixJob(), args);
        System.exit(ret);
    }
}
