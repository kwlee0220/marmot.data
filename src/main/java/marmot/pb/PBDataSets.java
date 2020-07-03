package marmot.pb;

import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.util.concurrent.CancellationException;

import org.slf4j.LoggerFactory;

import marmot.Column;
import marmot.DefaultRecord;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordStream;
import proto.RecordProto;
import proto.ValueProto;
import utils.async.AbstractThreadedExecution;
import utils.grpc.PBUtils;
import utils.io.IOUtils;
import utils.io.InputStreamFromOutputStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBDataSets {
	private PBDataSets() {
		throw new AssertionError("Should not be called here: class=" + getClass());
	}
	
	private static final int DEFAULT_PIPE_SIZE = 64 * 1024;
	public static InputStream toInputStream(RecordStream stream) {
		return new InputStreamFromOutputStream(os -> {
			WriteRecordSetToOutStream pump = new WriteRecordSetToOutStream(stream, os);
			pump.start();
			return pump;
		}, DEFAULT_PIPE_SIZE);
	}
	
	public static RecordProto toProto(Record record) {
		RecordProto.Builder builder = RecordProto.newBuilder();
		
		RecordSchema schema = record.getRecordSchema();
		for ( int i =0; i < schema.getColumnCount(); ++i ) {
			Column col = schema.getColumnAt(i);
			
			ValueProto vproto = PBValueProtos.toValueProto(col.type().typeClass(), record.get(i));
			builder.addColumn(vproto);
		}
		
		return builder.build();
	}
	
	private static class WriteRecordSetToOutStream extends AbstractThreadedExecution<Long> {
		private final RecordStream m_rset;
		private final OutputStream m_os;
		
		private WriteRecordSetToOutStream(RecordStream rset, OutputStream os) {
			m_rset = rset;
			m_os = os;
			
			setLogger(LoggerFactory.getLogger(WriteRecordSetToOutStream.class));
		}

		@Override
		protected Long executeWork() throws CancellationException, Exception {
			Record rec = DefaultRecord.of(m_rset.getRecordSchema());
			
			long count = 0;
			try {
				String typeId = m_rset.getRecordSchema().toTypeId();
				PBUtils.STRING(typeId).writeDelimitedTo(m_os);
				while ( m_rset.next(rec) ) {
					if ( !isRunning() ) {
						break;
					}
					
					toProto(rec).writeDelimitedTo(m_os);
					++count;
				}
				
				return count;
			}
			catch ( InterruptedIOException e ) {
				throw new CancellationException("" + e);
			}
			finally {
				m_rset.closeQuietly();
				IOUtils.closeQuietly(m_os);
			}
		}
	}
}
