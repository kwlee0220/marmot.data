package marmot.avro;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.RecordStreamException;
import marmot.stream.AbstractRecordStream;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AvroDeserializer extends AbstractRecordStream {
	private static final DecoderFactory FACT = DecoderFactory.get();
	
	private final Schema m_avroSchema;
	private final RecordSchema m_schema;
	private final InputStream m_is;

	private final DatumReader<GenericRecord> m_reader;
	private final BinaryDecoder m_decoder;
	private final AvroRecord m_record;
	
	public static RecordStream deserialize(RecordSchema schema, byte[] bytes) {
		ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
		return new AvroDeserializer(schema, bais);
	}
	
	public static RecordStream deserialize(RecordSchema schema, InputStream is) throws IOException {
		return new AvroDeserializer(schema, is);
	}
	
	public static RecordStream deserialize(RecordSchema schema, File file) throws IOException {
		InputStream is = new BufferedInputStream(new FileInputStream(file));
		return new AvroDeserializer(schema, is);
	}
	
	private AvroDeserializer(RecordSchema schema, InputStream is) {
		Utilities.checkNotNullArgument(schema);
		Utilities.checkNotNullArgument(is);
		
		m_avroSchema = AvroUtils.toSchema(schema);
		m_schema = schema;
		m_is = is;

		m_record = new AvroRecord(m_schema, m_avroSchema);
		m_reader = new GenericDatumReader<GenericRecord>(m_avroSchema);
		m_decoder = FACT.binaryDecoder(is, null);
	}

	@Override
	protected void closeInGuard() throws Exception {
		m_is.close();
	}

	public RecordSchema getRecordSchema() {
		return m_schema;
	}

	public Schema getAvroSchema() {
		return m_avroSchema;
	}
	
	@Override
	public Record next() {
		checkNotClosed();
		
		try {
			if ( m_decoder.isEnd() ) {
				return null;
			}

			m_reader.read(m_record.getGenericRecord(), m_decoder);
			return m_record;
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
