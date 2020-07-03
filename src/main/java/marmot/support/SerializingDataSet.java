package marmot.support;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import javax.annotation.Nullable;

import marmot.DataSetException;
import marmot.DefaultRecord;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.RecordStreamException;
import marmot.WritableDataSet;
import marmot.stream.AbstractRecordStream;
import marmot.type.DataType;
import marmot.type.RecordType;
import utils.func.Tuple;
import utils.io.IOUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class SerializingDataSet implements WritableDataSet {
	@Nullable private RecordSchema m_schema;
	
	protected abstract Tuple<RecordSchema,ObjectInputStream> getInputStream() throws IOException;
	protected abstract ObjectOutputStream getOutputStream(RecordSchema schema) throws IOException;
	
	public static FileDataSet from(File file) {
		return new FileDataSet(file);
	}
	
	public static BytesDataSet fromBytes() {
		return new BytesDataSet();
	}
	
	public static BytesDataSet fromBytes(byte[] bytes) {
		return new BytesDataSet(bytes);
	}
	
	private SerializingDataSet() {
		m_schema = null;
	}

	@Override
	public RecordSchema getRecordSchema() {
		if ( m_schema == null ) {
			try {
				Tuple<RecordSchema,ObjectInputStream> t = getInputStream();
				m_schema = t._1;
				IOUtils.closeQuietly(t._2);
			}
			catch ( IOException e ) {
				throw new DataSetException("fails to read RecordSchema: cause=" + e);
			}
		}
		
		return m_schema;
	}

	@Override
	public RecordStream read() {
		try {
			Tuple<RecordSchema,ObjectInputStream> t = getInputStream();
			if ( m_schema == null ) {
				m_schema = t._1;
			}

			return new StreamImpl(m_schema, t._2);
		}
		catch ( IOException e ) {
			throw new DataSetException("fails to open SerializableDataSet: cause=" + e);
		}
	}

	@Override
	public long write(RecordStream stream) {
		RecordSchema schema = stream.getRecordSchema();
		
		try ( ObjectOutputStream oos = getOutputStream(schema) ) {
			long count = 0;
			Record rec = DefaultRecord.of(schema);
			while ( stream.next(rec) ) {
				oos.writeByte(1);
				for ( int i =0; i < schema.getColumnCount(); ++i ) {
					DataType colType = schema.getColumnAt(i).type();
					colType.serialize(rec.get(i), oos);
				}
				++count;
			}
			oos.writeByte(0);
			
			return count;
		}
		catch ( IOException e ) {
			throw new DataSetException("" + e);
		}
	}

	private static class StreamImpl extends AbstractRecordStream {
		private final RecordSchema m_schema;
		private final ObjectInputStream m_ois;
		
		StreamImpl(RecordSchema schema, ObjectInputStream ois) {
			m_schema = schema;
			m_ois = ois;
		}
		
		@Override
		protected void closeInGuard() throws Exception {
			m_ois.close();
		}

		@Override
		public RecordSchema getRecordSchema() {
			return m_schema;
		}
		
		@Override
		public boolean next(Record output) {
			try {
				if ( m_ois.readByte() == 0 ) {
					return false;
				}
				else {
					m_schema.streamColumns()
							.forEachOrThrow(col -> output.set(col.ordinal(),
															col.type().deserialize(m_ois)));
					return true;
				}
			}
			catch ( IOException e ) {
				throw new RecordStreamException("" + e);
			}
		}
	}
	
	public static class FileDataSet extends SerializingDataSet {
		private final File m_file;
		
		private FileDataSet(File file) {
			m_file = file;
		}
		
		public File getFile() {
			return m_file;
		}
		
		@Override
		protected Tuple<RecordSchema, ObjectInputStream> getInputStream() throws IOException {
			ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(
																new FileInputStream(m_file)));
			String typeId = ois.readUTF();
			RecordSchema schema = ((RecordType)TypeParser.parseTypeId(typeId)).getRecordSchema();
			return Tuple.of(schema, ois);
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
	
	public static class BytesDataSet extends SerializingDataSet {
		@Nullable private byte[] m_bytes;
		
		private BytesDataSet(byte[] bytes) {
			m_bytes = bytes;
		}
		
		private BytesDataSet() {
		}
		
		public byte[] getBytes() {
			return m_bytes;
		}
		
		@Override
		protected Tuple<RecordSchema, ObjectInputStream> getInputStream() throws IOException {
			if ( m_bytes == null ) {
				throw new IllegalStateException("dataset bytes is not present");
			}
			
			ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(m_bytes));
			String typeId = ois.readUTF();
			RecordSchema schema = ((RecordType)TypeParser.parseTypeId(typeId)).getRecordSchema();
			return Tuple.of(schema, ois);
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
