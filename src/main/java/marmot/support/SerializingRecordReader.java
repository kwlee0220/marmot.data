package marmot.support;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

import javax.annotation.Nullable;

import marmot.DefaultRecord;
import marmot.Record;
import marmot.RecordReader;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.RecordStreamException;
import marmot.dataset.DataSetException;
import marmot.stream.AbstractRecordStream;
import marmot.type.RecordType;

import utils.Tuple;
import utils.io.IOUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class SerializingRecordReader implements RecordReader {
	@Nullable private RecordSchema m_schema;
	
	protected abstract Tuple<RecordSchema,ObjectInputStream> getInputStream() throws IOException;
	
	public static FileReader from(File file) {
		return new FileReader(file);
	}
	
	public static BytesReader fromBytes(byte[] bytes) {
		return new BytesReader(bytes);
	}
	
	private SerializingRecordReader() {
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

	private static class StreamImpl extends AbstractRecordStream {
		private final RecordSchema m_schema;
		private final ObjectInputStream m_ois;
		private final Record m_record;
		
		StreamImpl(RecordSchema schema, ObjectInputStream ois) {
			m_schema = schema;
			m_ois = ois;
			m_record = DefaultRecord.of(m_schema);
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
		public Record next() {
			try {
				if ( m_ois.readByte() == 0 ) {
					return null;
				}
				else {
					m_schema.streamColumns()
							.forEachOrThrow(col -> m_record.set(col.ordinal(),
																col.type().deserialize(m_ois)));
					return m_record;
				}
			}
			catch ( IOException e ) {
				throw new RecordStreamException("" + e);
			}
		}
	}
	
	public static class FileReader extends SerializingRecordReader {
		private final File m_file;
		
		private FileReader(File file) {
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
	}
	
	public static class BytesReader extends SerializingRecordReader {
		@Nullable private byte[] m_bytes;
		
		private BytesReader(byte[] bytes) {
			m_bytes = bytes;
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
	}
}
