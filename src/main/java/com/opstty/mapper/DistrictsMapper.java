package com.opstty.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DistrictsMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private boolean isFirstRow = true;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (isFirstRow) {
            isFirstRow = false;
            return; // Skip the first row (header)
        }

        String[] fields = value.toString().split(";");
        String district = fields[1];
        context.write(new Text(district), NullWritable.get());
    }
}
