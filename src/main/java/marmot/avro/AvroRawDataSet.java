package marmot.avro;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.RecordStreamException;
import marmot.WritableDataSet;
import marmot.stream.AbstractRecordStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class AvroRawDataSet implements WritableDataSet {
	private static final DecoderFactory FACT = DecoderFactory.get();
	
	private final Schema m_avroSchema;
	private final RecordSchema m_schema;
	
	protected abstract InputStream getInputStream() throws IOException;
	protected abstract OutputStream getOutputStream() throws IOException;
	
	public static AvroRawFileDataSet fromFile(RecordSchema schema, File file) {
		return new AvroRawFileDataSet(schema, file);
	}
	
	public static AvroRawBytesDataSet fromBytes(RecordSchema schema, byte[] bytes) {
		return new AvroRawBytesDataSet(schema, bytes);
	}
	
	public static AvroRawBytesDataSet fromBytes(RecordSchema schema) {
		return new AvroRawBytesDataSet(schema);
	}
	
	protected AvroRawDataSet(RecordSchema schema) {
		m_avroSchema = AvroUtils.toSchema(schema);
		m_schema = schema;
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	public static RecordStream read(RecordSchema schema, InputStream is) {
		return new StreamImpl(AvroUtils.toSchema(schema), is);
	}

	@Override
	public RecordStream read() {
		try {
			return new StreamImpl(m_avroSchema, getInputStream());
		}
		catch ( IOException e ) {
			throw new RecordStreamException("" + e);
		}
	}

	@Override
	public long write(RecordStream stream) {
		GenericDatumWriter<GenericRecord> m_datumWriter = new GenericDatumWriter<>(m_avroSchema);
		try ( OutputStream os = getOutputStream() ) {
			Encoder encoder = EncoderFactory.get().binaryEncoder(os, null);
			AvroRecord output = new AvroRecord(stream.getRecordSchema(), m_avroSchema);
			
			long count = 0;
			while ( stream.next(output) ) {
				m_datumWriter.write(output.getGenericRecord(), encoder);
				++count;
			}
			encoder.flush();
			
			return count;
		}
		catch ( IOException e ) {
			throw new RecordStreamException("" + e);
		}
	}
	
	private static class StreamImpl extends AbstractRecordStream {
		private final InputStream m_is;
		private final DatumReader<GenericRecord> m_reader;
		private final BinaryDecoder m_decoder;
		private final RecordSchema m_schema;
		private final GenericRecord m_record;
		
		private StreamImpl(Schema avroSchema, InputStream is) {
			m_is = is;
			
			m_schema = AvroUtils.toRecordSchema(avroSchema);
			m_record = new GenericData.Record(avroSchema);
			m_reader = new GenericDatumReader<GenericRecord>(avroSchema);
			m_decoder = FACT.binaryDecoder(is, null);
		}

		@Override
		protected void closeInGuard() throws Exception {
			m_is.close();
		}

		@Override
		public RecordSchema getRecordSchema() {
			return m_schema;
		}
		
		@Override
		public boolean next(Record output) {
			checkNotClosed();
			
			try {
				if ( m_decoder.isEnd() ) {
					return false;
				}
				
				if ( output instanceof AvroRecord ) {
					m_reader.read(((AvroRecord)output).getGenericRecord(), m_decoder);
				}
				else {
					m_reader.read(m_record, m_decoder);
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
			
			try {
				if ( m_decoder.isEnd() ) {
					return null;
				}
				
				return new AvroRecord(m_schema, m_reader.read(null, m_decoder));
			}
			catch ( Exception e ) {
				throw new RecordStreamException("" + e);
			}
		}
	}

	public static class AvroRawFileDataSet extends AvroRawDataSet {
		private final File m_file;
		
		private AvroRawFileDataSet(RecordSchema schema, File file) {
			super(schema);
			
			m_file = file;
		}
		
		public File getFile() {
			return m_file;
		}
		
		@Override
		protected InputStream getInputStream() throws IOException {
			return new BufferedInputStream(new FileInputStream(m_file));
		}

		@Override
		protected OutputStream getOutputStream() throws IOException {
			return new BufferedOutputStream(new FileOutputStream(m_file));
		}
	}

	public static class AvroRawBytesDataSet extends AvroRawDataSet {
		private volatile byte[] m_bytes;
		
		private AvroRawBytesDataSet(RecordSchema schema, byte[] bytes) {
			super(schema);
			
			m_bytes = bytes;
		}
		
		private AvroRawBytesDataSet(RecordSchema schema) {
			super(schema);
		}
		
		public byte[] getBytes() {
			return m_bytes;
		}
		
		@Override
		protected InputStream getInputStream() throws IOException {
			return new ByteArrayInputStream(m_bytes);
		}

		@Override
		protected OutputStream getOutputStream() throws IOException {
			return new DataSetOutputStream();
		}
		
		private final class DataSetOutputStream extends ByteArrayOutputStream {
			@Override
			public void close() throws IOException {
				super.close();
				
				m_bytes = toByteArray();
			}
		}
	}
}
