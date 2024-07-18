package com.opstty.mapper;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortHeightMapper extends Mapper<LongWritable, Text, FloatWritable, NullWritable> {
    private boolean isFirstRow = true;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (isFirstRow) {
            isFirstRow = false;
            return; // Skip the first row (header)
        }

        String[] fields = value.toString().split(";");
        if (fields.length > 6) {
            try {
                float height = Float.parseFloat(fields[6]);
                context.write(new FloatWritable(height), NullWritable.get());
            } catch (NumberFormatException e) {
                // Skip rows with invalid height value
            }
        }
    }
}
