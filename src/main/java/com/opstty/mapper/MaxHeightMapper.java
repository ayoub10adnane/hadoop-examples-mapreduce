package com.opstty.mapper;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxHeightMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
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
                String kind = fields[2];
                float height = Float.parseFloat(fields[6]);
                context.write(new Text(kind), new FloatWritable(height));
            } catch (NumberFormatException e) {
                // Skip rows with invalid height value
            }
        }
    }
}
