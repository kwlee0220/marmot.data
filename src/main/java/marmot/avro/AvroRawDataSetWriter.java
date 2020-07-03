package marmot.avro;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import marmot.DataSetWriter;
import marmot.Record;
import marmot.RecordStream;
import marmot.RecordStreamException;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class AvroRawDataSetWriter implements DataSetWriter {
	private final Schema m_avroSchema;
	private final OutputStream m_os;
	
	AvroRawDataSetWriter(Schema avroSchema, OutputStream os) {
		m_avroSchema = avroSchema;
		m_os = os;
	}

	@Override
	public long write(RecordStream stream) {
		GenericDatumWriter<GenericRecord> m_datumWriter = new GenericDatumWriter<>(m_avroSchema);
		try ( OutputStream os = m_os ) {
			Encoder encoder = EncoderFactory.get().binaryEncoder(os, null);
			Record output;
			
			long count = 0;
			while ( (output = stream.nextCopy()) != null ) {
				if ( output instanceof AvroRecord ) {
					m_datumWriter.write(((AvroRecord)output).getGenericRecord(), encoder);
				}
				else {
					m_datumWriter.write(AvroUtils.toGenericRecord(output, m_avroSchema), encoder);
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

}
