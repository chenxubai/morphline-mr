package com.github.minyk.morphlinesmr.mapper;

import com.github.minyk.morphlinesmr.counter.MorphlinesMRCounters;
import com.github.minyk.morphlinesmr.partitioner.ExceptionPartitioner;
import com.google.common.collect.ListMultimap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapperRecordEmitter implements Command {
    private static final Logger LOGGER = LoggerFactory.getLogger(MapperRecordEmitter.class);
    private final Text output_key = new Text();
    private final Text output_value = new Text();
    private final Mapper.Context context;
    private final Counter normal;
    private final Counter exception;

    public MapperRecordEmitter(Mapper.Context context) {

        this.context = context;
        this.exception = context.getCounter(MorphlinesMRCounters.COUNTERGROUP, MorphlinesMRCounters.Mapper.COUNTER_EXCEPTIOIN);
        this.normal = context.getCounter(MorphlinesMRCounters.COUNTERGROUP, MorphlinesMRCounters.Mapper.COUNTER_PROCESSED);
    }

    @Override
    public void notify(Record notification) {
    }

    @Override
    public Command getParent() {
        return null;
    }

    @Override
    public boolean process(Record record) {
    	if(record.get("out-key").isEmpty()) {
    		output_key.set(System.currentTimeMillis()+"");
    	}
        output_value.set(record.get("out-value").get(0).toString());
        
    	try {
            context.write(output_key, output_value);
            if(LOGGER.isDebugEnabled()) {
                LOGGER.debug("Check Exception: " + output_key.toString() + ", " + output_key.toString().startsWith(ExceptionPartitioner.EXCEPTION_KEY_VALUE));
            }
            if(output_key.toString().startsWith(ExceptionPartitioner.EXCEPTION_KEY_VALUE)) {
                exception.increment(1L);
            } else {
                normal.increment(1L);
            }
        } catch (Exception e) {
            LOGGER.warn("Cannot write record to context", e);
        }
        return true;
    }
}
