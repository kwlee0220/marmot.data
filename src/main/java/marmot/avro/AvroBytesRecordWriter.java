package marmot.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import marmot.RecordSchema;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AvroBytesRecordWriter extends AvroRecordWriter {
	private byte[] m_bytes;
	
	public AvroBytesRecordWriter() {
	}
	
	public byte[] getBytes() {
		return m_bytes;
	}

	@Override
	protected DataFileWriter<GenericRecord> getFileWriter(RecordSchema schema, Schema avroSchema) throws IOException {
		GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchema);
		DataFileWriter<GenericRecord> writer = new DataFileWriter<>(datumWriter);
		writer.setMeta("marmot_schema", schema.toString());
		getSyncInterval().transform(writer, DataFileWriter::setSyncInterval);
		getCodec().transform(writer, DataFileWriter::setCodec);
		writer.create(avroSchema, new DataSetOutputStream());
		
		return writer;
	}
	
	private final class DataSetOutputStream extends ByteArrayOutputStream {
		@Override
		public void close() throws IOException {
			super.close();
			
			m_bytes = toByteArray();
		}
	}
}