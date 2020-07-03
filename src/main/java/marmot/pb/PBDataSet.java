package marmot.pb;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.util.concurrent.CancellationException;

import javax.annotation.Nullable;

import marmot.DataSetException;
import marmot.DefaultRecord;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.RecordStreamException;
import marmot.WritableDataSet;
import marmot.stream.AbstractRecordStream;
import proto.RecordProto;
import proto.StringProto;
import utils.func.Tuple;
import utils.grpc.PBUtils;
import utils.io.IOUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class PBDataSet implements WritableDataSet {
	@Nullable private RecordSchema m_schema;
	
	protected abstract Tuple<RecordSchema,InputStream> getInputStream() throws IOException;
	protected abstract OutputStream getOutputStream(RecordSchema schema) throws IOException;
	
	public static FileDataSet from(File file) {
		return new FileDataSet(file);
	}
	
	public static BytesDataSet fromBytes() {
		return new BytesDataSet();
	}
	
	public static BytesDataSet fromBytes(byte[] bytes) {
		return new BytesDataSet(bytes);
	}

	@Override
	public RecordSchema getRecordSchema() {
		if ( m_schema == null ) {
			try {
				Tuple<RecordSchema,InputStream> t = getInputStream();
				m_schema = t._1;
				IOUtils.closeQuietly(t._2);
			}
			catch ( IOException e ) {
				throw new DataSetException("fails to read RecordSchema: cause=" + e);
			}
		}
		
		return m_schema;
	}

	public RecordStream read() {
		try {
			Tuple<RecordSchema,InputStream> t = getInputStream();
			if ( m_schema == null ) {
				m_schema = t._1;
			}

			return new PBRecordStream(m_schema, t._2);
		}
		catch ( IOException e ) {
			throw new DataSetException("fails to open SerializableDataSet: cause=" + e);
		}
	}

	@Override
	public long write(RecordStream stream) {
		Record rec = DefaultRecord.of(stream.getRecordSchema());
		
		long count = 0;
		try ( OutputStream out = getOutputStream(stream.getRecordSchema()) ) {
			while ( stream.next(rec) ) {
				PBDataSets.toProto(rec).writeDelimitedTo(out);
				++count;
			}
			
			return count;
		}
		catch ( InterruptedIOException e ) {
			throw new CancellationException("" + e);
		}
		catch ( IOException e ) {
			throw new RecordStreamException("" + e);
		}
	}

	private static class PBRecordStream extends AbstractRecordStream {
		private final RecordSchema m_schema;
		private final InputStream m_is;
		
		private PBRecordStream(RecordSchema schema, InputStream is) {
			m_schema = schema;
			m_is = is;
		}

		@Override
		protected void closeInGuard() {
			IOUtils.closeQuietly(m_is);
		}

		@Override
		public RecordSchema getRecordSchema() {
			return m_schema;
		}
		
		@Override
		public boolean next(Record output) {
			try {
				RecordProto proto = RecordProto.parseDelimitedFrom(m_is);
				if ( proto != null ) {
					for ( int i =0; i < m_schema.getColumnCount(); ++i ) {
						PBValueProtos.fromProto(proto.getColumn(i));
						output.set(i, PBValueProtos.fromProto(proto.getColumn(i)));
					}
					return true;
				}
				else {
					return false;
				}
			}
			catch ( IOException e ) {
				throw new RecordStreamException("" + e);
			}
		}
	}
	
	public static class FileDataSet extends PBDataSet {
		private final File m_file;
		
		private FileDataSet(File file) {
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

		@Override
		protected OutputStream getOutputStream(RecordSchema schema) throws IOException {
			OutputStream os = new FileOutputStream(m_file);
			PBUtils.STRING(schema.toTypeId()).writeDelimitedTo(os);
			os.flush();
			return os;
		}
	}
	
	public static class BytesDataSet extends PBDataSet {
		private byte[] m_bytes;
		
		private BytesDataSet() {
		}
		
		private BytesDataSet(byte[] bytes) {
			m_bytes = bytes;
		}
		
		public byte[] getBytes() {
			return m_bytes;
		}
		
		@Override
		protected Tuple<RecordSchema, InputStream> getInputStream() throws IOException {
			if ( m_bytes == null ) {
				throw new IllegalStateException("dataset bytes is not present");
			}
			InputStream is = new ByteArrayInputStream(m_bytes);
			
			String typeId = StringProto.parseDelimitedFrom(is).getValue();
			RecordSchema schema = RecordSchema.fromTypeId(typeId);
			
			return Tuple.of(schema, is);
		}

		@Override
		protected OutputStream getOutputStream(RecordSchema schema) throws IOException {
			PBOutputStream os = new PBOutputStream();
			PBUtils.STRING(schema.toTypeId()).writeDelimitedTo(os);
			os.flush();
			return os;
		}
		
		private final class PBOutputStream extends ByteArrayOutputStream {
			@Override
			public void close() throws IOException {
				super.close();
				
				m_bytes = toByteArray();
			}
		}
	}
}
