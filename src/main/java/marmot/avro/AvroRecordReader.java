package marmot.avro;

import java.io.IOException;

import javax.annotation.Nullable;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericRecord;

import marmot.Record;
import marmot.RecordReader;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.RecordStreamException;
import marmot.dataset.DataSetException;
import marmot.stream.AbstractRecordStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class AvroRecordReader implements RecordReader {
	@Nullable private RecordSchema m_schema;
	@Nullable private Schema m_avroSchema;
	
	protected abstract DataFileReader<GenericRecord> getFileReader() throws IOException;

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

	private static class StreamImpl extends AbstractRecordStream {
		private final DataFileReader<GenericRecord> m_fileReader;
		private final RecordSchema m_schema;
		private final Schema m_avroSchema;
		private final AvroRecord m_record;
		
		private StreamImpl(DataFileReader<GenericRecord> fileReader, RecordSchema schema) {
			m_fileReader = fileReader;
			m_schema = schema;
			m_avroSchema = fileReader.getSchema();
			m_record = new AvroRecord(m_schema, m_avroSchema);
		}

		@Override
		protected void closeInGuard() throws Exception {
			m_fileReader.close();
		}

		@Override
		public RecordSchema getRecordSchema() {
			return m_schema;
		}

		public Schema getAvroSchema() {
			return m_avroSchema;
		}
		
		@Override
		public Record next() {
			checkNotClosed();
			
			if ( !m_fileReader.hasNext() ) {
				return null;
			}
			
			try {
				m_fileReader.next(m_record.getGenericRecord());
				return m_record;
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
