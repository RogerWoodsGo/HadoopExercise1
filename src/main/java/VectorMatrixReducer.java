import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by beyondwu on 2016/3/15.
 */
public class VectorMatrixReducer extends Reducer<LongWritable, FloatWritable, LongWritable, FloatWritable> {
    @Override
    protected void reduce(LongWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context);
        Iterator<FloatWritable> value = values.iterator();
       // List<Float> twoMatrixValue = new ArrayList<Float>();
        float sum = 0;
        while (value.hasNext()) {
            sum += value.next().get();
            //twoMatrixValue.add(value.next().get());
        }
       // if(twoMatrixValue.size() == 2){
            context.write(key, new FloatWritable(sum));
       // }
    }
}
