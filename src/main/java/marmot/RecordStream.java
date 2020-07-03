package marmot;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import com.google.common.collect.Lists;

import io.reactivex.Observable;
import marmot.stream.AutoClosingRecordStream;
import marmot.stream.CloserAttachedRecordStream;
import marmot.stream.CountingRecordStream;
import marmot.stream.EmptyRecordStream;
import marmot.stream.FStreamChainedRecordStream;
import marmot.stream.FStreamRecordStream;
import marmot.stream.IteratorRecordStream;
import marmot.stream.PeekableRecordStream;
import marmot.stream.PipedRecordStream;
import marmot.stream.PushBackableRecordStream;
import marmot.stream.TakeRecordStream;
import utils.LoggerSettable;
import utils.Throwables;
import utils.Utilities;
import utils.func.CheckedConsumerX;
import utils.func.FOption;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface RecordStream extends AutoCloseable {
	public static final RecordStream NULL = empty(RecordSchema.NULL);
	
	public RecordSchema getRecordSchema();
	
	public void close();
	public boolean next(Record output);
	public Record nextCopy();
	
	public default void closeQuietly() {
		try {
			close();
		}
		catch ( Exception ignored ) { }
	}

	/**
	 * 빈 레코드세트 객체를 생성한다.
	 * 
	 * @param schema	레코드 세트의 스키마.
	 * @return		{@link RecordSet} 객체.
	 */
	public static RecordStream empty(RecordSchema schema) {
		Utilities.checkNotNullArgument(schema, "RecordSchema");
		
		return new EmptyRecordStream(schema);
	}
	
	public default RecordStream onClose(Runnable closer) {
		Utilities.checkNotNullArgument(closer, "Closer");
		
		return new CloserAttachedRecordStream(this, closer);
	}
	
	public default RecordStream take(long count) {
		return new TakeRecordStream(this, count);
	}

	/**
	 * 단일 레코드로 구성된 레코드 세트를 생성한다.
	 * 
	 * @param records	레코드 세트를 구성할 레코드 리스트.
	 * @return	레코드 세트
	 */
	public static RecordStream of(Record... records) {
		Utilities.checkNotNullArgument(records, "records is null");
		Utilities.checkArgument(records.length > 0, "records.length > 0, but: " + records.length);
		
		RecordSchema schema = records[0].getRecordSchema();
		return from(schema, Arrays.asList(records));
	}
	
	public static PeekableRecordStream toPeekable(RecordStream stream) {
		return new PeekableRecordStream(stream);
	}

	/**
	 * 레코드 스트림에 속한 레코드로부터 레코드 세트를 생성한다.
	 * 
	 * @param schema	레코드 스크마.
	 * @param fstream	레코드 스트림
	 * @return	레코드 세트
	 */
	public static RecordStream from(RecordSchema schema, FStream<? extends Record> fstream) {
		Utilities.checkNotNullArgument(schema, "RecordSchema is null");
		Utilities.checkNotNullArgument(fstream, "FStream is null");
		
		return new FStreamRecordStream(schema, fstream);
	}

	/**
	 * 주어진 레코드들로 구성된 레코드 세트를 생성한다.
	 * <p>
	 * 레코드 집합에는 반드시 하나 이상의 레코드가 포함되어야 한다.
	 * 
	 * @param records	레코드 세트에 포함될 레코드 집합.
	 * @return	레코드 세트
	 */
	public static RecordStream from(Iterable<? extends Record> records) {
		Utilities.checkNotNullArgument(records, "records is null");
		
		Iterator<? extends Record> iter = records.iterator();
		Utilities.checkArgument(iter.hasNext(), "Record Iterable is empty");
		
		RecordSchema schema = iter.next().getRecordSchema();
		return from(schema, records.iterator());
	}

	/**
	 * 주어진 레코드들로 구성된 레코드 세트를 생성한다.
	 * <p>
	 * 올바른 동작을 위해서는 인자인 {@code schema}와 레코드들의 스키마는 동일하여야 한다.
	 * 
	 * @param schema	생성될 레코드 세트의 스키마.
	 * @param iter	레코드 세트에 포함될 레코드 집합.
	 * @return	레코드 세트
	 */
	public static RecordStream from(RecordSchema schema, Iterable<? extends Record> iter) {
		Utilities.checkNotNullArgument(schema, "schema is null");
		Utilities.checkNotNullArgument(iter, "records is null");
		
		return from(schema, iter.iterator());
	}

	/**
	 * 주어진 레코드의 Iterator로부터 레코드 세트를 생성한다.
	 * <p>
	 * 올바른 동작을 위해서는 인자인 {@code schema}와 레코드들의 스키마는 동일하여야 한다.
	 * 
	 * @param schema	생성될 레코드 세트의 스키마.
	 * @param iter	레코드 세트에 포함될 레코드 집합.
	 * @return	레코드 세트
	 */
	public static RecordStream from(RecordSchema schema, Iterator<? extends Record> iter) {
		Utilities.checkNotNullArgument(schema, "schema is null");
		Utilities.checkNotNullArgument(iter, "records is null");
		
		return new IteratorRecordStream(schema, iter);
	}
	
	public static PipedRecordStream pipe(RecordSchema schema, int queueLength) {
		return new PipedRecordStream(schema, queueLength);
	}
	
	public static RecordStream from(RecordSchema schema, Observable<? extends Record> records,
									int queueLength) {
		PipedRecordStream pipe = new PipedRecordStream(schema, queueLength);
		records.subscribe(pipe::supply, pipe::endOfSupply, pipe::endOfSupply);
		
		return pipe;
	}
	
	public static RecordStream concat(RecordSchema schema, FStream<? extends RecordStream> streams) {
		Utilities.checkNotNullArgument(schema, "schema is null");
		Utilities.checkNotNullArgument(streams, "RecordStreams are null");
		
		return FStreamChainedRecordStream.from(schema, streams);
	}
	
	public static RecordStream concat(RecordStream... streams) {
		Utilities.checkNotNullArguments(streams, "RecordStreams are null");
		
		return concat(streams[0].getRecordSchema(), FStream.of(streams));
	}
	
	public static RecordStream concat(RecordStream rset, Record tail) {
		Utilities.checkNotNullArgument(rset, "rset is null");
		Utilities.checkNotNullArgument(tail, "tail is null");
		Utilities.checkArgument(rset.getRecordSchema().equals(tail.getRecordSchema()),
								"incompatible RecordSchema");
		
		return concat(rset, RecordStream.of(tail));
	}
	
	public static RecordStream concat(Record head, RecordStream tail) {
		Utilities.checkNotNullArgument(head, "head is null");
		Utilities.checkNotNullArgument(tail, "tails is null");
		
		return concat(RecordStream.of(head), tail);
	}
	
	/**
	 * 본 레코드 세트에 포함된 레코드를 접근하는 스트림 ({@link FStream})을 반환한다.
	 * 
	 * @return	레코드 세트 스트림 객체.
	 */
	public default FStream<Record> fstream() {
		return new FStream<Record>() {
			@Override
			public void close() {
				RecordStream.this.close();
			}

			@Override
			public FOption<Record> next() {
				return FOption.ofNullable(RecordStream.this.nextCopy());
			}
		};
	}
	
	public default RecordStream asAutoCloseable() {
		return new AutoClosingRecordStream(this);
	}
	
	public default FOption<RecordStream> asNonEmpty() {
		PushBackableRecordStream pushable = asPushBackable();
		
		if ( pushable.peekCopy() != null ) {
			return FOption.of((RecordStream)pushable);
		}
		else {
			return FOption.empty();
		}
	}
	
	public default PushBackableRecordStream asPushBackable() {
		return (this instanceof PushBackableRecordStream)
				? (PushBackableRecordStream)this
				: new PushBackableRecordStream(this);
	}
	
	public default CountingRecordStream asCountingRecordSet() {
		return new CountingRecordStream(this);
	}
	
	public default <S> S foldLeft(S accum, S stopper,
								BiFunction<? super S,? super Record,? extends S> folder) {
		Utilities.checkNotNullArgument(folder, "folder is null");

		if ( accum.equals(stopper) ) {
			return accum;
		}

		try {
			Record record = DefaultRecord.of(getRecordSchema());
			while ( next(record) ) {
				accum = folder.apply(accum, record);
				if ( accum.equals(stopper) ) {
					return accum;
				}
			}
			
			return accum;
		}
		finally {
			closeQuietly();
		}
	}
	
	public default <S> S collectLeft(S accum, BiConsumer<? super S,? super Record> consumer) {
		Utilities.checkNotNullArgument(consumer, "consumer is null");

		try {
			Record record = DefaultRecord.of(getRecordSchema());
			while ( next(record) ) {
				consumer.accept(accum, record);
			}
			
			return accum;
		}
		finally {
			closeQuietly();
		}
	}
	
	public default <S> S collectLeftCopy(S accum,
										BiConsumer<? super S,? super Record> consumer) {
		Utilities.checkNotNullArgument(consumer, "consumer is null");

		try {
			Record record;
			while ( (record = nextCopy()) != null ) {
				consumer.accept(accum, record);
			}
			
			return accum;
		}
		finally {
			closeQuietly();
		}
	}
	
	/**
	 * 레코드 세트에 포함된 모든 레코드로 구성된 리스트를 생성한다.
	 * <p>
	 * 전체 레코드들을 읽어 리스트가 구성되면 자동으로 {@link #close()}가 호출된다.
	 * 
	 * @return	레코드 리스트.
	 */
	public default List<Record> toList() {
		return collectLeftCopy(Lists.newArrayList(), (c,r) -> c.add(r));
	}
	
	public default FOption<Record> findFirst() {
		try {
			Record record = nextCopy();
			return record != null ? FOption.of(record) : FOption.empty();
		}
		finally {
			closeQuietly();
		}
	}
	
	/**
	 * 본 레코드 세트에 포함된 레코드에 대해 차례대로 주어진
	 * {@link Consumer#accept(Object)}를 호출한다.
	 * <p>
	 * {@link Consumer#accept(Object)} 호출 중 오류가 발생되는 경우는 무시되고,
	 * 다음 레코드로 진행된다.
	 * 
	 * @param consumer	레코드 세트에 포함된 레코드를 처리할 레코드 소비자 객체.
	 */
	public default void forEach(Consumer<? super Record> consumer) {
		Utilities.checkNotNullArgument(consumer, "consumer is null");
		
		Record record = DefaultRecord.of(getRecordSchema());
		try {
			while ( next(record) ) {
				try {
					consumer.accept(record);
				}
				catch ( Throwable e ) {
					if ( this instanceof LoggerSettable ) {
						((LoggerSettable)this).getLogger()
												.warn("fails to consume record: " + record, e);
					}
				}
			}
		}
		catch ( Throwable e ) {
			Throwables.sneakyThrow(e);
		}
		finally {
			closeQuietly();
		}
	}
	
	/**
	 * 본 레코드 세트에 포함된 레코드에 대해 차례대로 주어진
	 * {@link Consumer#accept(Object)}를 호출한다.
	 * <p>
	 * {@link Consumer#accept(Object)} 호출 중 오류가 발생되는 경우는 무시되고,
	 * 다음 레코드로 진행된다.
	 * 
	 * @param consumer	레코드 세트에 포함된 레코드를 처리할 레코드 소비자 객체.
	 */
	public default void forEachCopy(Consumer<? super Record> consumer) {
		Utilities.checkNotNullArgument(consumer, "consumer is null");

		try {
			Record record;
			while ( (record = nextCopy()) != null ) {
				try {
					consumer.accept(record);
				}
				catch ( Throwable e ) {
					if ( this instanceof LoggerSettable ) {
						((LoggerSettable)this).getLogger().warn("fails to consume record: " + record, e);
					}
				}
			}
		}
		catch ( Throwable e ) {
			Throwables.sneakyThrow(e);
		}
		finally {
			closeQuietly();
		}
	}
	
	public default <X extends Throwable>
	void forEachCopyOrThrow(CheckedConsumerX<? super Record, X> consumer) throws X {
		Utilities.checkNotNullArgument(consumer, "consumer is null");
		
		Record record;
		try {
			while ( (record = nextCopy()) != null ) {
				consumer.accept(record);
			}
		}
		finally {
			closeQuietly();
		}
	}
	
	public default long count() {
		Record record = DefaultRecord.of(getRecordSchema());
		
		try {
			long cnt = 0;
			while ( next(record) ) {
				++cnt;
			}
			
			return cnt;
		}
		finally {
			closeQuietly();
		}
	}
}
