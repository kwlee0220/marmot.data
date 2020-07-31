package marmot.pb;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import marmot.RecordSchema;
import utils.grpc.PBUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBFileRecordWriter extends PBRecordWriter {
	private final File m_file;
	private final RecordSchema m_schema;
	
	public PBFileRecordWriter(File file, RecordSchema schema) {
		m_file = file;
		m_schema = schema;
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	public File getFile() {
		return m_file;
	}

	@Override
	protected OutputStream getOutputStream(RecordSchema schema) throws IOException {
		OutputStream os = new FileOutputStream(m_file);
		PBUtils.STRING(schema.toTypeId()).writeDelimitedTo(os);
		os.flush();
		return os;
	}
}