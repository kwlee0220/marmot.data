package marmot.stream;

import java.util.Date;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import utils.Throwables;
import utils.Utilities;
import utils.async.Guard;
import utils.async.GuardedRunnable;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordStreamClosedException;
import marmot.RecordStreamException;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PipedRecordStream extends AbstractRecordStream {
	private static final Logger s_logger = LoggerFactory.getLogger(PipedRecordStream.class);
	
	private static final long DEFAULT_TIMEOUT_MILLIS = TimeUnit.MINUTES.toMillis(5);	// 5m
	enum State {
		/** 작동 중인 상태.*/
		OPEN,
		/** Supplier가 더 이상의 레코드 생성이 완료되어 더 이상의 레코드 추가가 멈춘 상태. */
		SUPPLIER_CLOSED,
		/** Consumer가 (오류 발생으로) 더이상 데이터를 읽지 않게된 상태 */
		CONSUMER_CLOSED,
		/** 레코드 채널이 폐쇄된 상태. */
		CLOSED
	};

	private final RecordSchema m_schema;
	private final int m_maxQueueLength;
	private long m_supplierTimeout = DEFAULT_TIMEOUT_MILLIS;
	private long m_consumerTimeout = DEFAULT_TIMEOUT_MILLIS;
	
	private final Guard m_guard = Guard.create();
	@GuardedBy("m_guard") private State m_state = State.OPEN;
	@GuardedBy("m_guard") private Throwable m_failure = null;
	@GuardedBy("m_guard") private final Queue<Record> m_queue;
	@GuardedBy("m_guard") private long m_lastSupplyMillis;
	@GuardedBy("m_guard") private long m_lastConsumeMillis;
	
	public PipedRecordStream(RecordSchema schema, int queueLength) {
		Utilities.checkNotNullArgument(schema, "schema is null");
		Preconditions.checkArgument(queueLength > 0, "queue length should be larger than zero");
		
		m_schema = schema;
		m_maxQueueLength = queueLength;
		m_queue = new ArrayBlockingQueue<Record>(queueLength);
		
		m_lastSupplyMillis = m_lastConsumeMillis = System.currentTimeMillis();
		
		setLogger(s_logger);
	}
	
	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	public int getLength() {
		return m_queue.size();
	}
	
	public int getMaxQueueLength() {
		return m_maxQueueLength;
	}
	
	public boolean isConsumerClosed() {
		m_guard.lock();
		try {
			return m_state == State.CONSUMER_CLOSED || m_state == State.CLOSED;
		}
		finally {
			m_guard.unlock();
		}
	}
	
	public boolean isSupplierClosed() {
		m_guard.lock();
		try {
			return m_state == State.SUPPLIER_CLOSED || m_state == State.CLOSED;
		}
		finally {
			m_guard.unlock();
		}
	}
	
	public boolean isSupplierTimeout() {
		m_guard.lock();
		try {
			return (System.currentTimeMillis() - m_lastSupplyMillis) > m_supplierTimeout; 
		}
		finally {
			m_guard.unlock();
		}
	}
	
	public boolean isConsumerTimeout() {
		m_guard.lock();
		try {
			return (System.currentTimeMillis() - m_lastConsumeMillis) > m_consumerTimeout; 
		}
		finally {
			m_guard.unlock();
		}
	}
	
	public long getSupplierTimeout() {
		return m_supplierTimeout;
	}
	
	public PipedRecordStream setSupplierTimeout(long timeout) {
		m_supplierTimeout = timeout;
		return this;
	}
	
	public long getConsumerTimeout() {
		return m_consumerTimeout;
	}
	
	public PipedRecordStream setConsumerTimeout(long timeout) {
		m_consumerTimeout = timeout;
		return this;
	}
	
	public PipedRecordStream setTimeout(long timeout) {
		m_supplierTimeout = timeout;
		m_consumerTimeout = timeout;
		return this;
	}
	
	@Override
	protected void closeInGuard() {
		m_guard.lock();
		try {
			switch ( m_state ) {
				case OPEN:
					m_state = State.CONSUMER_CLOSED;
					m_guard.signalAllInGuard();
					break;
				case SUPPLIER_CLOSED:
					m_state = State.CLOSED;
					m_guard.signalAllInGuard();
					break;
				default:
					break;
			}
		}
		finally {
			m_guard.unlock();
		}
	}
	
	@Override
	public Record next() {
		return nextCopy();
	}
	
	@Override
	public Record nextCopy() {
		checkNotClosed();
		
		m_guard.lock();
		try {
			Date due = new Date(m_lastSupplyMillis + m_supplierTimeout);
			while ( true ) {
				switch ( m_state ) {
					case OPEN:
					case SUPPLIER_CLOSED:
						if ( m_failure != null ) {
							Throwables.throwIfInstanceOf(m_failure, RecordStreamException.class);
							throw new RecordStreamException("" + m_failure);
						}
						break;
					case CLOSED:
						throw new RecordStreamClosedException("already closed");
					case CONSUMER_CLOSED:
						throw new IllegalStateException("state=" + State.CONSUMER_CLOSED);
				}

				Record record = m_queue.poll();
				if ( record != null ) {
					m_lastConsumeMillis = System.currentTimeMillis();
					m_guard.signalAllInGuard();
					
					return record;
				}
				if ( m_state == State.SUPPLIER_CLOSED ) {
					throw new NoSuchElementException();
				}
				
				if ( !m_guard.awaitUntilInGuard(due) ) {
					throw new RecordStreamTimeoutException("PipedRecordSet supplier is too slow");
				}
			}
		}
		catch ( InterruptedException e ) {
			throw new RecordStreamException("" + e);
		}
		finally {
			m_guard.unlock();
		}
	}

	public boolean supply(Record record) {
		m_guard.lock();
		try {
			Date due = new Date(m_lastConsumeMillis + m_consumerTimeout);
			while ( true ) {
				switch ( m_state ) {
					case OPEN:
						if ( m_queue.offer(record) ) {
							m_lastSupplyMillis = System.currentTimeMillis();
							getLogger().debug("append a record: {}/{}",
												m_queue.size(), m_maxQueueLength);
							
							m_guard.signalAllInGuard();
							return true;
						}
						if ( !m_guard.awaitUntilInGuard(due) ) {
							throw new RecordStreamTimeoutException("PipedRecordSet consumer is too slow");
						}
						break;
					case CLOSED:
					case SUPPLIER_CLOSED:
						throw new IllegalStateException("expected state=" + State.OPEN
														+ ", state=" + m_state);
					case CONSUMER_CLOSED:
						return false;
				}
			}
		}
		catch ( InterruptedException e ) {
			throw new RecordStreamException("" + e);
		}
		finally {
			m_guard.unlock();
		}
	}
	
	public void endOfSupply() {
		GuardedRunnable.from(m_guard, () -> {
			switch ( m_state ) {
				case OPEN:
					m_state = State.SUPPLIER_CLOSED;
					break;
				case CONSUMER_CLOSED:
					m_state = State.CLOSED;
					break;
				default:
					throw new IllegalStateException("unexpected state: state=" + m_state);
			}
		}).run();
	}
	
	public void endOfSupply(Throwable failure) {
		GuardedRunnable.from(m_guard, () -> {
			switch ( m_state ) {
				case OPEN:
					m_state = State.SUPPLIER_CLOSED;
					m_failure = failure;
					break;
				case CONSUMER_CLOSED:
					m_state = State.CLOSED;
					m_failure = failure;
					break;
				default:
					throw new IllegalStateException("unexpected state: state=" + m_state);
			}
			m_guard.signalAllInGuard();
		}).run();
	}
	
	public void waitForFullyClosed() throws InterruptedException {
		m_guard.awaitUntil(() -> m_state == State.CLOSED);
	}
	
	public boolean waitForFullyClosed(long timeout, TimeUnit unit) throws InterruptedException {
		return m_guard.awaitUntil(() -> m_state == State.CLOSED, timeout, unit);
	}
	
	@Override
	public String toString() {
		return String.format("pipe_rset[%s,%d/%d]", ""+m_state, getLength(), getMaxQueueLength());
	}
	
	public static class ProgressReport {
		private long m_supplyCount;
		private long m_consumeCount;
		
		private ProgressReport() {
			this(0, 0);
		}
		
		private ProgressReport(long supplyCount, long consumeCount) {
			m_supplyCount = supplyCount;
			m_consumeCount = consumeCount;
		}
		
		public long getSupplyCount() {
			return m_supplyCount;
		}
		
		public long getConsumeCount() {
			return m_consumeCount;
		}
		
		@Override
		public String toString() {
			return String.format("supply=%d, consume=%d", m_supplyCount, m_consumeCount);
		}
		
		private long getStep() {
			return m_supplyCount + m_consumeCount;
		}
		
		private ProgressReport notifySupplied() {
			return new ProgressReport(m_supplyCount+1, m_consumeCount);
		}
		
		private ProgressReport notifyConsumed() {
			return new ProgressReport(m_supplyCount, m_consumeCount+1);
		}
	}
}
