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
	
	public static FileWriter from(File file) {
		return new FileWriter(file);
	}
	
	public static BytesWriter fromBytes() {
		return new BytesWriter();
	}

	@Override
	public void write(RecordStream stream) {
		RecordSchema schema = stream.getRecordSchema();
		
		try ( ObjectOutputStream oos = getOutputStream(schema) ) {
			Record record;
			while ( (record = stream.next()) != null ) {
				oos.writeByte(1);
				for ( int i =0; i < schema.getColumnCount(); ++i ) {
					DataType colType = schema.getColumnAt(i).type();
					colType.serialize(record.get(i), oos);
				}
			}
			oos.writeByte(0);
		}
		catch ( IOException e ) {
			throw new DataSetException("" + e);
		}
	}
	
	public static class FileWriter extends SerializingRecordWriter {
		private final File m_file;
		
		private FileWriter(File file) {
			m_file = file;
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
		@Nullable private byte[] m_bytes;
		
		private BytesWriter() {
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
