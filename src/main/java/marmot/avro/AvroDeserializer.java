package marmot.avro;

import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordStreamException;
import marmot.stream.AbstractRecordStream;

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
	private final GenericRecord m_record;
	
	public static AvroDeserializer from(RecordSchema schema, InputStream is) {
		return new AvroDeserializer(schema, is);
	}
	
	private AvroDeserializer(RecordSchema schema, InputStream is) {
		m_avroSchema = AvroUtils.toSchema(schema);
		m_schema = schema;
		m_is = is;

		m_record = new GenericData.Record(m_avroSchema);
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

	public Schema getAvroStream() {
		return m_avroSchema;
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
				AvroUtils.copyToRecord(m_record, output);
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
