package com.github.minyk.morphlinesmr;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.github.minyk.morphlinesmr.mapper.MorphlinesMapper;
import com.github.minyk.morphlinesmr.partitioner.ExceptionPartitioner;
import com.github.minyk.morphlinesmr.reducer.IdentityReducer;

/**
 * Created by drake on 10/10/14.
 */
public class MorphlineMRExceptionTest {
    MapReduceDriver<ImmutableBytesWritable, KeyValue, Text, Text, NullWritable, Text> driver;

    @Before
    public void setUp() throws URISyntaxException {
        MorphlinesMapper mapper = new MorphlinesMapper();
        IdentityReducer reducer = new IdentityReducer();
        ExceptionPartitioner partitioner = new ExceptionPartitioner();
        driver = new MapReduceDriver<ImmutableBytesWritable, KeyValue, Text, Text, NullWritable, Text>(mapper, reducer);

        URL file = MorphlineMRExceptionTest.class.getClassLoader().getResource("morphline_with_exception.conf");
        driver.addCacheFile(file.toURI());
        
        driver.getConfiguration().set(MorphlinesMRConfig.MORPHLINE_FILE,file.getPath());
        driver.getConfiguration().setBoolean(MorphlinesMRConfig.MORPHLINE_FILE_TEST, true);
        driver.getConfiguration().set(MorphlinesMRConfig.MORPHLINE_ID,"morphline1");
    }

    @Test
    public void testNormalCase() {
        driver.withInput(new ImmutableBytesWritable(), new KeyValue());
        driver.withOutput(NullWritable.get(), new Text("2943974000,syslog,sshd,listening on 0.0.0.0 port 22."));
        try {
            driver.runTest();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testExceptionCase() {
        driver.withInput(new ImmutableBytesWritable(), new KeyValue());
        driver.withOutput(NullWritable.get(), new Text("<>Feb  4 10:46:14 syslog sshd[607]: listening on 0.0.0.0 port 22."));
        try {
            driver.runTest();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
