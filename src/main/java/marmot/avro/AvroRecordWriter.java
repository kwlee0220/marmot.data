package marmot.avro;

import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.CancellationException;

import javax.annotation.Nullable;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.RecordStreamException;
import marmot.RecordWriter;
import utils.LoggerSettable;
import utils.StopWatch;
import utils.UnitUtils;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class AvroRecordWriter implements RecordWriter, LoggerSettable {
	private static final Logger s_logger = LoggerFactory.getLogger(AvroRecordWriter.class);
	
	private static final int DEFAULT_SYNC_INTERVAL = DataFileConstants.DEFAULT_SYNC_INTERVAL * 2;
	private static final CodecFactory CODEC_FACT = CodecFactory.snappyCodec();
	
	@Nullable private Integer m_syncInterval = DEFAULT_SYNC_INTERVAL;
	@Nullable private CodecFactory m_codec = CODEC_FACT;
	private Logger m_logger = s_logger;
	
	protected abstract DataFileWriter<GenericRecord> getFileWriter(RecordSchema schema, Schema avroSchema)
		throws IOException;
	
	public static AvroRecordWriter into(File file) {
		return new AvroFileRecordWriter(file);
	}
	
	public FOption<Integer> getSyncInterval() {
		return FOption.ofNullable(m_syncInterval);
	}
	
	public AvroRecordWriter setSyncInterval(int interval) {
		m_syncInterval = interval;
		return this;
	}
	
	public AvroRecordWriter setSyncInterval(String interval) {
		return setSyncInterval((int)UnitUtils.parseByteSize(interval));
	}
	
	public FOption<CodecFactory> getCodec() {
		return FOption.ofNullable(m_codec);
	}
	
	public AvroRecordWriter setCodec(CodecFactory fact) {
		m_codec = fact;
		return this;
	}

	@Override
	public Logger getLogger() {
		return m_logger;
	}

	@Override
	public void setLogger(Logger logger) {
		m_logger = logger;
	}
	
	@Override
	public long write(RecordStream stream) {
		RecordSchema schema = stream.getRecordSchema();
		Schema avroSchema = AvroUtils.toSchema(schema);
		
		StopWatch watch = StopWatch.start();
		try ( DataFileWriter<GenericRecord> writer = getFileWriter(schema, avroSchema) ) {
			writer.setFlushOnEveryBlock(false);
			
			long count = 0;
			for ( Record record = stream.next(); record != null; record = stream.next() ) {
				if ( record instanceof AvroRecord ) {
					GenericRecord grec = ((AvroRecord)record).getGenericRecord();
					writer.append(grec);
				}
				else {
					GenericRecord grec = AvroUtils.toGenericRecord(record, avroSchema);
					writer.append(grec);
				}
				++count;
			}
			writer.flush();
			
			if ( getLogger().isInfoEnabled() ) {
				watch.stop();
				long velo = Math.round(writer.sync() / watch.getElapsedInFloatingSeconds());
				getLogger().info("written {}: size={}, elapsed={}, velo={}/s",
								this, UnitUtils.toByteSizeString(writer.sync()),
								watch.getElapsedMillisString(), UnitUtils.toByteSizeString(velo));
			}
			
			return count;
		}
		catch ( InterruptedIOException e ) {
			throw new CancellationException("" + e);
		}
		catch ( IOException e ) {
			throw new RecordStreamException("" + e);
		}
	}
}
