package marmot.avro;

import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.CancellationException;

import javax.annotation.Nullable;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.RecordStreamException;
import marmot.RecordWriter;
import utils.UnitUtils;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class AvroRecordWriter implements RecordWriter {
	@Nullable private Integer m_syncInterval;
	@Nullable private CodecFactory m_codec;
	
	protected abstract DataFileWriter<GenericRecord> getFileWriter(RecordSchema schema, Schema avroSchema)
		throws IOException;
	
	public static AvroRecordWriter into(File file) {
		return new AvroFileRecordWriter(file);
	}
	
	public static AvroRecordWriter into() {
		return new AvroBytesRecordWriter();
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
	public void write(RecordStream stream) {
		RecordSchema schema = stream.getRecordSchema();
		Schema avroSchema = AvroUtils.toSchema(schema);
		
		try ( DataFileWriter<GenericRecord> writer = getFileWriter(schema, avroSchema) ) {
			Record record;
			while ( (record = stream.next()) != null ) {
				if ( record instanceof AvroRecord ) {
					GenericRecord grec = ((AvroRecord)record).getGenericRecord();
					writer.append(grec);
				}
				else {
					GenericRecord grec = AvroUtils.toGenericRecord(record, avroSchema);
					writer.append(grec);
				}
			}
			writer.flush();
		}
		catch ( InterruptedIOException e ) {
			throw new CancellationException("" + e);
		}
		catch ( IOException e ) {
			throw new RecordStreamException("" + e);
		}
	}
}
