package com.github.minyk.morphlinesmr.mapper;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.minyk.morphlinesmr.MorphlinesMRConfig;
import com.github.minyk.morphlinesmr.partitioner.ExceptionPartitioner;

public class MorphlinesMapper extends Mapper<ImmutableBytesWritable, KeyValue, Text, Text> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MorphlinesMapper.class);
    public static final String EXCEPTION_KEY_FIELD = "exceptionkey";
    private final Record record = new Record();
    private Command morphline;
    boolean useReducers;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());

        File morphLineFile = new File(paths[0].toString());
        String morphLineId = context.getConfiguration().get(MorphlinesMRConfig.MORPHLINE_ID);
        MapperRecordEmitter recordEmitter = new MapperRecordEmitter(context);
        MorphlineContext morphlineContext = new MorphlineContext.Builder().build();
        morphline = new org.kitesdk.morphline.base.Compiler()
                .compile(morphLineFile, morphLineId, morphlineContext, recordEmitter);
        if(context.getConfiguration().get(MorphlinesMRConfig.MORPHLINESMR_REDUCERS_EXCEPTION, "0").equals("0")) {
            useReducers = false;
        } else {
            useReducers = true;
        }
    }

    public void map(ImmutableBytesWritable key, KeyValue value, Context context) throws IOException, InterruptedException {

    	if(!"pbc".equals(Bytes.toString(CellUtil.cloneQualifier(value)))) {
    		return;
    	}
		
		record.put(Fields.ATTACHMENT_BODY, new ByteArrayInputStream(CellUtil.cloneValue(value)));
		record.put("key", Bytes.toString(CellUtil.cloneRow(value)));
		
		if(LOGGER.isDebugEnabled()) {
			LOGGER.debug("Value: " + value.toString());
		}
		if(useReducers) {
			record.put(EXCEPTION_KEY_FIELD, ExceptionPartitioner.EXCEPTION_KEY_VALUE);
		}
		
		if (!morphline.process(record)) {
			LOGGER.info("Morphline failed to process record: {}", record);
		}
		
		//record.removeAll(Fields.ATTACHMENT_BODY);
		record.getFields().clear();
	
    }
}
