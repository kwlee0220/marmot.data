package marmot.avro;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.CancellationException;

import javax.annotation.Nullable;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.file.SeekableFileInput;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.commons.io.FileUtils;

import marmot.DataSetException;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.RecordStreamException;
import marmot.WritableDataSet;
import marmot.stream.AbstractRecordStream;
import utils.UnitUtils;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class AvroDataSet implements WritableDataSet {
	private RecordSchema m_schema;
	private Schema m_avroSchema;
	@Nullable private Integer m_syncInterval;
	@Nullable private CodecFactory m_codec;
	
	protected abstract DataFileReader<GenericRecord> getFileReader() throws IOException;
	protected abstract DataFileWriter<GenericRecord> getFileWriter(RecordSchema schema)
		throws IOException;
	
	public static AvroDataSet from(File file) {
		return new AvroFileDataSet(file);
	}
	
	public static AvroBytesDataSet fromBytes() {
		return new AvroBytesDataSet();
	}
	
	public static AvroBytesDataSet fromBytes(byte[] bytes) {
		return new AvroBytesDataSet(bytes);
	}
	
	public FOption<Integer> getSyncInterval() {
		return FOption.ofNullable(m_syncInterval);
	}
	
	public AvroDataSet setSyncInterval(int interval) {
		m_syncInterval = interval;
		return this;
	}
	
	public AvroDataSet setSyncInterval(String interval) {
		return setSyncInterval((int)UnitUtils.parseByteSize(interval));
	}
	
	public FOption<CodecFactory> getCodec() {
		return FOption.ofNullable(m_codec);
	}
	
	public AvroDataSet setCodec(CodecFactory fact) {
		m_codec = fact;
		return this;
	}

	@Override
	public RecordSchema getRecordSchema() {
		if ( m_schema == null ) {
			try ( DataFileReader<GenericRecord> reader = getFileReader() ) {
				m_avroSchema = reader.getSchema();
				m_schema = AvroUtils.toRecordSchema(m_avroSchema);
			}
			catch ( IOException e ) {
				throw new DataSetException("fails to read RecordSchema: cause=" + e);
			}
		}
		
		return m_schema;
	}
	
	public Schema getAvroSchema() {
		if ( m_schema == null ) {
			try ( DataFileReader<GenericRecord> reader = getFileReader() ) {
				m_avroSchema = reader.getSchema();
				m_schema = AvroUtils.toRecordSchema(m_avroSchema);
			}
			catch ( IOException e ) {
				throw new DataSetException("fails to read RecordSchema: cause=" + e);
			}
		}
		
		return m_avroSchema;
	}

	@Override
	public RecordStream read() {
		try {
			DataFileReader<GenericRecord> reader = getFileReader();
			if ( m_schema == null ) {
				m_avroSchema = reader.getSchema();
				m_schema = AvroUtils.toRecordSchema(m_avroSchema);
			}
			
			return new StreamImpl(reader, m_schema);
		}
		catch ( IOException e ) {
			throw new DataSetException("fails to open SerializableDataSet: cause=" + e);
		}
	}

	@Override
	public long write(RecordStream stream) {
		RecordSchema schema = stream.getRecordSchema();
		Schema avroSchema = AvroUtils.toSchema(schema);
		
		long count = 0;
		AvroRecord output = new AvroRecord(schema, avroSchema);
		try ( DataFileWriter<GenericRecord> writer = getFileWriter(schema) ) {
			while ( stream.next(output) ) {
				writer.append(output.getGenericRecord());
				++count;
			}
			writer.flush();
			
			return count;
		}
		catch ( InterruptedIOException e ) {
			throw new CancellationException("" + e);
		}
		catch ( IOException e ) {
			throw new RecordStreamException("" + e);
		}
	}

	private static class AvroFileDataSet extends AvroDataSet {
		private final File m_file;
		
		private AvroFileDataSet(File file) {
			m_file = file;
		}
		
		public File getFile() {
			return m_file;
		}
		
		@Override
		protected DataFileReader<GenericRecord> getFileReader() throws IOException {
			SeekableInput input = new SeekableFileInput(m_file);
			
			DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
			return new DataFileReader<>(input, reader);
		}
	
		@Override
		protected DataFileWriter<GenericRecord> getFileWriter(RecordSchema schema) throws IOException {
			Schema avroSchema = AvroUtils.toSchema(schema);
			
			FileUtils.forceMkdirParent(m_file);
			
			GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchema);
			DataFileWriter<GenericRecord> writer = new DataFileWriter<>(datumWriter);
			writer.setMeta("marmot_schema", schema.toString());
			getSyncInterval().transform(writer, DataFileWriter::setSyncInterval);
			getCodec().transform(writer, DataFileWriter::setCodec);
			writer.create(avroSchema, m_file);
			
			return writer;
		}
	}
	
	public static class AvroBytesDataSet extends AvroDataSet {
		private byte[] m_bytes;
		
		private AvroBytesDataSet() {
		}
		
		private AvroBytesDataSet(byte[] bytes) {
			m_bytes = bytes;
		}
		
		public byte[] getBytes() {
			return m_bytes;
		}
		
		@Override
		protected DataFileReader<GenericRecord> getFileReader() throws IOException {
			SeekableInput input = new SeekableByteArrayInput(m_bytes);
			
			DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
			return new DataFileReader<>(input, reader);
		}

		@Override
		protected DataFileWriter<GenericRecord> getFileWriter(RecordSchema schema) throws IOException {
			Schema avroSchema = AvroUtils.toSchema(schema);
			
			GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchema);
			DataFileWriter<GenericRecord> writer = new DataFileWriter<>(datumWriter);
			writer.setMeta("marmot_schema", schema.toString());
			getSyncInterval().transform(writer, DataFileWriter::setSyncInterval);
			getCodec().transform(writer, DataFileWriter::setCodec);
			writer.create(avroSchema, new DataSetOutputStream());
			
			return writer;
		}
		
		private final class DataSetOutputStream extends ByteArrayOutputStream {
			@Override
			public void close() throws IOException {
				super.close();
				
				m_bytes = toByteArray();
			}
		}
	}

	private static class StreamImpl extends AbstractRecordStream {
		private final DataFileReader<GenericRecord> m_fileReader;
		private final RecordSchema m_schema;
		private final GenericRecord m_record;
		
		private StreamImpl(DataFileReader<GenericRecord> fileReader, RecordSchema schema) {
			m_fileReader = fileReader;
			m_schema = schema;
			m_record = new GenericData.Record(fileReader.getSchema());
		}

		@Override
		protected void closeInGuard() throws Exception {
			m_fileReader.close();
		}

		@Override
		public RecordSchema getRecordSchema() {
			return m_schema;
		}
		
		@Override
		public boolean next(Record output) {
			checkNotClosed();
			
			if ( !m_fileReader.hasNext() ) {
				return false;
			}
			
			try {
				if ( output instanceof AvroRecord ) {
					m_fileReader.next(((AvroRecord)output).getGenericRecord());
				}
				else {
					m_fileReader.next(m_record);
					for ( int i =0; i < m_schema.getColumnCount(); ++i ) {
						output.set(i, m_record.get(i));
					}
				}
				
				return true;
			}
			catch ( IOException e ) {
				throw new RecordStreamException("" + e);
			}
		}
		
		@Override
		public Record nextCopy() {
			checkNotClosed();
			
			if ( !m_fileReader.hasNext() ) {
				return null;
			}
			
			try {
				return new AvroRecord(m_schema, m_fileReader.next());
			}
			catch ( Exception e ) {
				throw new RecordStreamException("" + e);
			}
		}
	}
}
