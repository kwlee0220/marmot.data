package marmot.avro;

import java.io.OutputStream;

import org.apache.avro.Schema;

import marmot.DataSetWriter;
import marmot.RecordStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AvroSerializer implements DataSetWriter {
	private final Schema m_avroSchema;
	private final OutputStream m_os;
	
	public AvroSerializer(Schema avroSchema, OutputStream os) {
		m_avroSchema = avroSchema;
		m_os = os;
	}

	@Override
	public long write(RecordStream stream) {
		return 0;
	}
}
