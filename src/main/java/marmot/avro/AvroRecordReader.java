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
	private @Nullable RecordSchema m_schema;
	private @Nullable Schema m_avroSchema;
	
	protected abstract DataFileReader<GenericRecord> getFileReader() throws IOException;
	
	protected AvroRecordReader() { }
	protected AvroRecordReader(RecordSchema schema, Schema avroSchema) {
		m_schema = schema;
		m_avroSchema = avroSchema;
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
			
			return new StreamImpl(reader);
		}
		catch ( IOException e ) {
			throw new DataSetException("fails to open SerializableDataSet: cause=" + e);
		}
	}
	
	class StreamImpl extends AbstractRecordStream {
		private final DataFileReader<GenericRecord> m_fileReader;
		private final AvroRecord m_record;
		
		StreamImpl(DataFileReader<GenericRecord> fileReader) {
			m_fileReader = fileReader;
			m_record = new AvroRecord(m_schema, fileReader.getSchema());
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
				// 새로운 레코드를 읽을 것이기 때문에, 기존 캐쉬에 있는 값을 모두 제거한다.
				m_record.clearCache();
				
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
