package com.opstty.reducer;

import com.opstty.writable.AgeDistrictWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OldestTreeReducer extends Reducer<IntWritable, AgeDistrictWritable, IntWritable, AgeDistrictWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<AgeDistrictWritable> values, Context context) throws IOException, InterruptedException {
        AgeDistrictWritable oldest = null;
        for (AgeDistrictWritable val : values) {
            if (oldest == null || val.getAge().get() > oldest.getAge().get()) {
                oldest = val;
            }
        }
        context.write(key, oldest);
    }
}
