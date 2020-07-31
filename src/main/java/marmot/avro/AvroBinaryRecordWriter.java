package marmot.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.RecordStreamException;
import marmot.RecordWriter;
import utils.Utilities;
import utils.io.IOUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AvroBinaryRecordWriter implements RecordWriter {
	private final RecordSchema m_schema;
	private final Schema m_avroSchema;
	private final OutputStream m_os;
	
	public AvroBinaryRecordWriter(RecordSchema schema, Schema avroSchema, OutputStream os) {
		Utilities.checkNotNullArgument(avroSchema);
		Utilities.checkNotNullArgument(os);

		m_schema = schema;
		m_avroSchema = avroSchema;
		m_os = os;
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}

	@Override
	public long write(RecordStream stream) {
		Utilities.checkNotNullArgument(stream, "Input RecordStream");
		
		GenericDatumWriter<GenericRecord> m_writer = new GenericDatumWriter<>(m_avroSchema);
		try {
			Encoder encoder = EncoderFactory.get().binaryEncoder(m_os, null);
			
			long count = 0;
			for ( Record record = stream.next(); record != null; record = stream.next() ) {
				if ( record instanceof AvroRecord ) {
					GenericRecord grec = ((AvroRecord)record).getGenericRecord();
					m_writer.write(grec, encoder);
				}
				else {
					GenericRecord grec = AvroUtils.toGenericRecord(record, m_avroSchema);
					m_writer.write(grec, encoder);
				}
				++count;
			}
			encoder.flush();
			
			return count;
		}
		catch ( IOException e ) {
			throw new RecordStreamException("" + e);
		}
	}
	
	public static byte[] writeToBytes(RecordStream stream) {
		Schema avroSchema = AvroUtils.toSchema(stream.getRecordSchema());
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			AvroBinaryRecordWriter ser = new AvroBinaryRecordWriter(stream.getRecordSchema(), avroSchema, baos);
			ser.write(stream);
		}
		finally {
			IOUtils.closeQuietly(baos);
		}
		
		return baos.toByteArray();
	}
}
