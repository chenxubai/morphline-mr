package com.github.minyk.morphlinesmr.commands;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import com.github.minyk.morphlinesmr.MorphlinesMRConfig;
import com.github.minyk.morphlinesmr.mapper.MorphlinesMapper;

public class MorphinesMapperArithmeticTest {

    MapDriver<ImmutableBytesWritable, KeyValue, Text, Text> mapDriver;

    @Before
    public void setUp() throws URISyntaxException {
        MorphlinesMapper mapper = new MorphlinesMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
        URL file = MorphlinesMapperCSVTest.class.getClassLoader().getResource("morphline_arithmetic.conf");
        mapDriver.addCacheFile(file.toURI());
        mapDriver.getConfiguration().set(MorphlinesMRConfig.MORPHLINE_FILE, file.getPath());
        mapDriver.getConfiguration().setBoolean(MorphlinesMRConfig.MORPHLINE_FILE_TEST, true);
        mapDriver.getConfiguration().set(MorphlinesMRConfig.MORPHLINE_ID, "morphline1");
    }

    @Test
    public void test() {
        mapDriver.clearInput();
        mapDriver.withInput(new ImmutableBytesWritable(), new KeyValue());
//        mapDriver.withInput(new LongWritable(0), new Text("AAA,34,BB"));
        mapDriver.withOutput(new Text("1"), new Text("134"));
        try {
            mapDriver.runTest();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
