package marmot.pb;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.util.concurrent.CancellationException;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.RecordStreamException;
import marmot.RecordWriter;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class PBRecordWriter implements RecordWriter {
	protected abstract OutputStream getOutputStream(RecordSchema schema) throws IOException;

	@Override
	public long write(RecordStream stream) {
		try ( OutputStream out = getOutputStream(stream.getRecordSchema()) ) {
			long count = 0;
			for ( Record record = stream.next(); record != null; record = stream.next() ) {
				PBRecordStreams.toProto(record).writeDelimitedTo(out);
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
}
