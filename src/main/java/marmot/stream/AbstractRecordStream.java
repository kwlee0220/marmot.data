package marmot.stream;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.RecordStream;
import marmot.RecordStreamClosedException;
import utils.LoggerSettable;
import utils.Throwables;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class AbstractRecordStream implements RecordStream, LoggerSettable {
	private Logger m_logger = LoggerFactory.getLogger(getClass());
	
	private AtomicBoolean m_closed = new AtomicBoolean(false);
	
	protected abstract void closeInGuard() throws Exception;
	
	public final boolean isClosed() {
		return m_closed.get();
	}

	@Override
	public final void close() {
		if ( m_closed.compareAndSet(false, true) ) {
			try {
				closeInGuard();
			}
			catch ( Throwable e ) {
				getLogger().warn("fails to close RecordSet: " + this
								+ ", cause=" + Throwables.unwrapThrowable(e));
			}
			finally {
				m_closed.set(true);
			}
		}
	}

	@Override
	public Logger getLogger() {
		return m_logger;
	}

	@Override
	public void setLogger(Logger logger) {
		m_logger = logger;
	}
	
	protected final void checkNotClosed() {
		if ( isClosed() ) {
			throw new RecordStreamClosedException("already closed: this=" + getClass());
		}
	}
}
