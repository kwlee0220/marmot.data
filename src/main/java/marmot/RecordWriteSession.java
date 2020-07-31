package marmot;

import java.util.concurrent.CompletableFuture;

import marmot.stream.PipedRecordStream;
import utils.Throwables;
import utils.async.Guard;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RecordWriteSession implements AutoCloseable {
	private final PipedRecordStream m_pipe;
	private final Guard m_guard = Guard.create();
	private RecordStreamException m_cause;
	
	RecordWriteSession(RecordWriter writer) {
		m_pipe = new PipedRecordStream(writer.getRecordSchema(), 16);

		CompletableFuture.runAsync(() -> {
			try {
				writer.write(m_pipe);
			}
			catch ( Exception e ) {
				m_guard.run(() -> {
					if ( e instanceof RecordStreamException ) {
						m_cause = (RecordStreamException)e;
					}
					else {
						m_cause = new RecordStreamException("fails to write a Record", e);
					}
				});
			}
		});
	}

	@Override
	public void close() throws Exception {
		m_pipe.endOfSupply();
	}
	
	public void write(Record record) {
		if ( m_guard.get(() -> m_cause) != null ) {
			throw m_cause;
		}
		
		try {
			m_pipe.supply(record);
		}
		catch ( Exception e ) {
			m_pipe.endOfSupply(e);
			
			Throwables.throwIfInstanceOf(e, RecordStreamException.class);
			throw new RecordStreamException("fails to supply a Record", e);
		}
	}
}
