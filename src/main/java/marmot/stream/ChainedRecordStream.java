package marmot.stream;

import javax.annotation.Nullable;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.RecordStreamException;
import utils.io.IOUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class ChainedRecordStream extends AbstractRecordStream {
	private @Nullable RecordStream m_current = null;	// null이면 first-call 의미
	private boolean m_eos = false;

	/**
	 * 레코드 스키마 객체를 반환한다.
	 * 
	 * @return	레코드 스키마
	 */
	abstract public RecordSchema getRecordSchema();
	
	/**
	 * 다음으로 접근할 레코드 세트 객체를 반환한다.
	 * <p>
	 * 만일 더 이상의 레코드 세트가 없는 경우는 {@code null}을 반환한다.
	 * 
	 * @return 레코드 세트 객체 또는 {@code null}.
	 */
	abstract protected RecordStream getNextRecordStream();

	@Override
	protected void closeInGuard() throws Exception {
		IOUtils.closeQuietly(m_current);
		
		super.close();
	}

	@Override
	public Record next() throws RecordStreamException {
		checkNotClosed();
		
		if ( m_eos ) {
			return null;
		}
		
		// first-call?
		if ( m_current == null ) { 
			if ( (m_current = getNextRecordStream()) == null ) {
				m_eos = true;
				return null;
			}
		}
		
		Record record;
		while ( (record = m_current.next()) == null ) {
			m_current.closeQuietly();
			 
			if ( (m_current = getNextRecordStream()) == null ) {
				m_eos = true;
				return null;
			}
			
			if ( !getRecordSchema().equals(m_current.getRecordSchema()) ) {
				throw new RecordStreamException("Component RecordSchema is incompatible to the merged one: "
											+ "concated=" + getRecordSchema()
											+ ", component=" + m_current.getRecordSchema());
			}
		}
		
		return record;
	}
}