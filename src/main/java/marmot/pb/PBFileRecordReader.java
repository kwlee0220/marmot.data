package marmot.pb;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import marmot.RecordSchema;
import marmot.proto.StringProto;
import utils.func.Tuple;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBFileRecordReader extends PBRecordReader {
	private final File m_file;
	
	public PBFileRecordReader(File file) {
		m_file = file;
	}
	
	public File getFile() {
		return m_file;
	}
	
	@Override
	protected Tuple<RecordSchema, InputStream> getInputStream() throws IOException {
		InputStream is = new FileInputStream(m_file);
		
		String typeId = StringProto.parseDelimitedFrom(is).getValue();
		RecordSchema schema = RecordSchema.fromTypeId(typeId);
		
		return Tuple.of(schema, is);
	}
}