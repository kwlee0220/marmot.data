package marmot.avro;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AvroDeserializer {
	private static final DecoderFactory DEC_FACT = DecoderFactory.get();

	private final Schema m_avroSchema;
	private final DatumReader<GenericRecord> m_reader;
	private BinaryDecoder m_decoder;
	private final GenericRecord m_record;
	
	public AvroDeserializer(Schema schema) {
		m_avroSchema = schema;
		m_record = new GenericData.Record(schema);
		m_reader = new GenericDatumReader<>(schema);
	}
	
	public Schema getAvroSchema() {
		return m_avroSchema;
	}
	
	public GenericRecord deserialize(byte[] bytes) throws IOException {
		InputStream is = new ByteArrayInputStream(bytes);
		m_decoder = DEC_FACT.binaryDecoder(is, m_decoder);
		return m_reader.read(m_record, m_decoder);
	}
}
