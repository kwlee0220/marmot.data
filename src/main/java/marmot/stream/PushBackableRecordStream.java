package marmot.stream;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.RecordStreamException;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PushBackableRecordStream extends AbstractRecordStream {
	private final RecordStream m_input;
	private final List<Record> m_pushBackeds;
	
	public PushBackableRecordStream(RecordStream rset) {
		Utilities.checkNotNullArgument(rset, "rset is null");
		
		m_input = rset;
		m_pushBackeds = new ArrayList<>();
	}
	
	@Override
	protected void closeInGuard() {
		m_input.closeQuietly();
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_input.getRecordSchema();
	}

	@Override
	public Record next() throws RecordStreamException {
		checkNotClosed();
		
		if ( !m_pushBackeds.isEmpty() ) {
			return m_pushBackeds.remove(m_pushBackeds.size()-1);
		}
		else {
			return m_input.next();
		}
	}
	
	@Override
	public Record nextCopy() {
		checkNotClosed();

		if ( !m_pushBackeds.isEmpty() ) {
			return m_pushBackeds.remove(m_pushBackeds.size()-1);
		}
		else {
			return m_input.nextCopy();
		}
	}
	
	public boolean peek(Record output) {
		Utilities.checkNotNullArgument(output, "output is null");
		
		Record next = nextCopy();
		if ( next != null ) {
			pushBack(next);
		}
		
		return next != null;
	}
	
	public Record peekCopy() {
		Record next = nextCopy();
		if ( next != null ) {
			pushBack(next);
		}
		
		return next;
	}

	public PushBackableRecordStream pushBack(Record record) {
		checkNotClosed();
		Preconditions.checkArgument(m_input.getRecordSchema().equals(record.getRecordSchema()),
									"Push-backed record is incompatible");
		
		m_pushBackeds.add(record.duplicate());
		return this;
	}
	
	@Override
	public String toString() {
		return m_input.toString();
	}
}
