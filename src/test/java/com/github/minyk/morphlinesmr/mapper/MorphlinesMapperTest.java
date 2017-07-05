package com.github.minyk.morphlinesmr.mapper;

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
import com.github.minyk.morphlinesmr.partitioner.ExceptionPartitioner;


/**
 * Created by drake on 9/16/14.
 */
public class MorphlinesMapperTest {

    MapDriver<ImmutableBytesWritable, KeyValue, Text, Text> mapDriver;

    @Before
    public void setUp() throws URISyntaxException {
        MorphlinesMapper mapper = new MorphlinesMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
        URL file = MorphlinesMapperTest.class.getClassLoader().getResource("morphline_with_exception.conf");
        mapDriver.addCacheFile(file.toURI());
        mapDriver.getConfiguration().set(MorphlinesMRConfig.MORPHLINE_FILE,file.getPath());
        mapDriver.getConfiguration().setBoolean(MorphlinesMRConfig.MORPHLINE_FILE_TEST, true);
        mapDriver.getConfiguration().set(MorphlinesMRConfig.MORPHLINE_ID, "morphline1");
        mapDriver.getConfiguration().set("exceptionkey", ExceptionPartitioner.EXCEPTION_KEY_VALUE);
    }

    @Test
    public void testNormalCase() {
        mapDriver.clearInput();
        mapDriver.withInput(new ImmutableBytesWritable(), new KeyValue());
//        mapDriver.withInput(new LongWritable(0), new Text("Feb  4 10:46:14 syslog sshd[607]: listening on 0.0.0.0 port 22."));
        mapDriver.withOutput(new Text("syslog,sshd"), new Text("2943974000,syslog,sshd,listening on 0.0.0.0 port 22."));
        try {
            mapDriver.runTest();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testExceptionCase() {
        mapDriver.clearInput();
        mapDriver.withInput(new ImmutableBytesWritable(), new KeyValue());
//        mapDriver.withInput(new LongWritable(0), new Text("<>Feb  4 10:46:14 syslog sshd[607]: listening on 0.0.0.0 port 22."));
        mapDriver.withOutput(new Text(ExceptionPartitioner.EXCEPTION_KEY_VALUE), new Text("<>Feb  4 10:46:14 syslog sshd[607]: listening on 0.0.0.0 port 22."));
        try {
            mapDriver.runTest();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
