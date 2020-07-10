package marmot.avro;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;

import marmot.RecordSchema;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AvroFileRecordWriter extends AvroRecordWriter {
	private final File m_file;
	
	public AvroFileRecordWriter(File file) {
		m_file = file;
	}
	
	public File getFile() {
		return m_file;
	}

	@Override
	protected DataFileWriter<GenericRecord> getFileWriter(RecordSchema schema, Schema avroSchema) throws IOException {
		FileUtils.forceMkdirParent(m_file);
		
		GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchema);
		DataFileWriter<GenericRecord> writer = new DataFileWriter<>(datumWriter);
		writer.setMeta("marmot_schema", schema.toString());
		getSyncInterval().transform(writer, DataFileWriter::setSyncInterval);
		getCodec().transform(writer, DataFileWriter::setCodec);
		writer.create(avroSchema, m_file);
		
		return writer;
	}
}