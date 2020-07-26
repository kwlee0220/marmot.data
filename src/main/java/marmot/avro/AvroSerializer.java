package marmot.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AvroSerializer {
	private static final EncoderFactory ENC_FACT = EncoderFactory.get();

	private final Schema m_avroSchema;
	private final GenericDatumWriter<GenericRecord> m_writer;
	private BinaryEncoder m_encoder;
	
	public AvroSerializer(Schema schema) {
		m_avroSchema = schema;
		m_writer = new GenericDatumWriter<>(m_avroSchema);
	}
	
	public Schema getAvroSchema() {
		return m_avroSchema;
	}

	public void serialize(GenericRecord record, OutputStream os) throws IOException {
		m_encoder = ENC_FACT.binaryEncoder(os, m_encoder);
		m_writer.write(record, m_encoder);
		m_encoder.flush();
	}

	public byte[] serializeToBytes(GenericRecord record) {
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			m_encoder = EncoderFactory.get().binaryEncoder(baos, m_encoder);
			m_writer.write(record, m_encoder);
			m_encoder.flush();
			
			return baos.toByteArray();
		}
		catch ( IOException e ) {
			throw new AssertionError(e);
		}
	}
}
