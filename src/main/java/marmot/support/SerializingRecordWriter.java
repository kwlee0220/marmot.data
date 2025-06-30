package marmot.support;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import javax.annotation.Nullable;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.RecordWriter;
import marmot.dataset.DataSetException;
import marmot.type.DataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class SerializingRecordWriter implements RecordWriter {
	protected abstract ObjectOutputStream getOutputStream(RecordSchema schema) throws IOException;
	
	public static FileWriter from(File file, RecordSchema schema) {
		return new FileWriter(file, schema);
	}
	
	public static BytesWriter fromBytes(RecordSchema schema) {
		return new BytesWriter(schema);
	}

	@Override
	public long write(RecordStream stream) {
		RecordSchema schema = stream.getRecordSchema();
		
		try ( ObjectOutputStream oos = getOutputStream(schema) ) {
			long count = 0;
			for ( Record record = stream.next(); record != null; record = stream.next() ) {
				oos.writeByte(1);
				for ( int i =0; i < schema.getColumnCount(); ++i ) {
					DataType colType = schema.getColumnAt(i).type();
					colType.serialize(record.get(i), oos);
				}
			}
			oos.writeByte(0);
			
			return count;
		}
		catch ( IOException e ) {
			throw new DataSetException("" + e);
		}
	}
	
	public static class FileWriter extends SerializingRecordWriter {
		private final RecordSchema m_schema;
		private final File m_file;
		
		private FileWriter(File file, RecordSchema schema) {
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
		protected ObjectOutputStream getOutputStream(RecordSchema schema) throws IOException {
			ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(
																	new FileOutputStream(m_file)));
			oos.writeUTF(DataType.RECORD(schema).id());
			oos.flush();
			return oos;
		}
	}
	
	public static class BytesWriter extends SerializingRecordWriter {
		private final RecordSchema m_schema;
		private @Nullable byte[] m_bytes;
		
		private BytesWriter(RecordSchema schema) {
			m_schema = schema;
		}

		@Override
		public RecordSchema getRecordSchema() {
			return m_schema;
		}
		
		public byte[] getBytes() {
			return m_bytes;
		}

		@Override
		protected ObjectOutputStream getOutputStream(RecordSchema schema) throws IOException {
			SerializingOutputStream oos = new SerializingOutputStream(new ByteArrayOutputStream());
			oos.writeUTF(DataType.RECORD(schema).id());
			oos.flush();
			return oos;
		}
		
		private final class SerializingOutputStream extends ObjectOutputStream {
			private final ByteArrayOutputStream m_baos;
			
			protected SerializingOutputStream(ByteArrayOutputStream baos)
				throws IOException, SecurityException {
				super(baos);
				
				m_baos = baos;
			}
			
			@Override
			public void close() throws IOException {
				super.close();
				
				m_bytes = m_baos.toByteArray();
			}
		}
	}
}
