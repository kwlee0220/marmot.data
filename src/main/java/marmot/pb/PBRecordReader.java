package marmot.pb;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import javax.annotation.Nullable;

import marmot.DefaultRecord;
import marmot.Record;
import marmot.RecordReader;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.RecordStreamException;
import marmot.dataset.DataSetException;
import marmot.proto.RecordProto;
import marmot.stream.AbstractRecordStream;

import utils.Tuple;
import utils.io.IOUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class PBRecordReader implements RecordReader {
	@Nullable private RecordSchema m_schema;
	
	protected abstract Tuple<RecordSchema,InputStream> getInputStream() throws IOException;
	
	public static PBFileRecordReader from(File file) {
		return new PBFileRecordReader(file);
	}
	
	public static PBBytesRecordReader fromBytes(byte[] bytes) {
		return new PBBytesRecordReader(bytes);
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

	private static class PBRecordStream extends AbstractRecordStream {
		private final RecordSchema m_schema;
		private final InputStream m_is;
		private final Record m_output;
		
		private PBRecordStream(RecordSchema schema, InputStream is) {
			m_schema = schema;
			m_is = is;
			m_output = DefaultRecord.of(m_schema);
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
		public Record next() {
			try {
				RecordProto proto = RecordProto.parseDelimitedFrom(m_is);
				if ( proto != null ) {
					for ( int i =0; i < m_schema.getColumnCount(); ++i ) {
						PBValueProtos.fromProto(proto.getColumn(i));
						m_output.set(i, PBValueProtos.fromProto(proto.getColumn(i)));
					}
					return m_output;
				}
				else {
					return null;
				}
			}
			catch ( IOException e ) {
				throw new RecordStreamException("" + e);
			}
		}
	}
}
