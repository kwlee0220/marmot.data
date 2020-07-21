package marmot;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;

import com.google.common.collect.Maps;

import io.reactivex.Observable;
import io.reactivex.ObservableEmitter;
import io.reactivex.ObservableOnSubscribe;
import marmot.optor.FStreamConcatedDataSet;
import marmot.optor.FilteredReader;
import marmot.optor.MultiColumnKey;
import marmot.optor.PeekingDataSet;
import marmot.optor.ProjectedReader;
import marmot.optor.RenameRecordReader;
import marmot.optor.ScriptFilteredReader;
import marmot.optor.TakenReader;
import marmot.optor.geo.TransformSridReader;
import marmot.support.RecordListDataSource;
import marmot.support.RecordScript;
import utils.Utilities;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface RecordReader {
	public RecordSchema getRecordSchema();

	public RecordStream read();
	
	public default RecordReader cache() {
		RecordListDataSource.Writer writer = RecordListDataSource.writer(getRecordSchema());
		writer.write(read());
		
		return RecordListDataSource.reader(getRecordSchema(), writer.getRecordList());
	}
	
	public default RecordReader filter(Predicate<? super Record> pred) {
		return new FilteredReader(this, pred);
	}

	public default RecordReader filterScript(String predicate) {
		return filterScript(RecordScript.of(predicate));
	}
	public default RecordReader filterScript(RecordScript predicate) {
		return new ScriptFilteredReader(this, predicate);
	}

	public default RecordReader project(String... cols) {
		return new ProjectedReader(this, MultiColumnKey.of(cols));
	}
	public default RecordReader project(List<String> cols) {
		return new ProjectedReader(this, MultiColumnKey.ofNames(cols));
	}
	
	public default RecordReader take(long count) {
		return new TakenReader(this, count);
	}
	
	public default RecordReader peek(Consumer<Record> action) {
		return new PeekingDataSet(this, action);
	}
	
	public default RecordReader transformSrid(String tarSrid) {
		return new TransformSridReader(this, "the_geom", tarSrid);
	}
	
	public default RecordReader renameColumn(String srcColName, String tarColName) {
		Map<String,String> mappings = Maps.newHashMap();
		mappings.put(srcColName, tarColName);
		
		return new RenameRecordReader(this, mappings);
	}
	
	public static RecordReader concat(RecordSchema schema, FStream<? extends RecordReader> datasets) {
		Utilities.checkNotNullArgument(schema, "schema is null");
		Utilities.checkNotNullArgument(datasets, "DataSet stream is null");
		
		return new FStreamConcatedDataSet(schema, datasets);
	}
	
	public static RecordReader concat(RecordReader... datasets) {
		Utilities.checkNotNullArguments(datasets, "Datasets are null");
		
		return concat(datasets[0].getRecordSchema(), FStream.of(datasets));
	}
	
	public static RecordReader concat(Iterable<? extends RecordReader> datasets) {
		Utilities.checkNotNullArgument(datasets, "rsets is null");
		
		Iterator<? extends RecordReader> iter = datasets.iterator();
		if ( !iter.hasNext() ) {
			throw new IllegalArgumentException("rset is empty");
		}
		return concat(iter.next().getRecordSchema(), FStream.from(datasets));
	}
	
	public default Observable<Record> observe() {
		return Observable.create(new ObservableOnSubscribe<Record>() {
			@Override
			public void subscribe(ObservableEmitter<Record> emitter) throws Exception {
				Record record;
				try ( RecordStream stream = RecordReader.this.read() ) {
					while ( (record = stream.nextCopy()) != null ) {
						if ( emitter.isDisposed() ) {
							return;
						}
						emitter.onNext(record);
					}
					
					if ( emitter.isDisposed() ) {
						return;
					}
					emitter.onComplete();
				}
				catch ( Throwable e ) {
					emitter.onError(e);
				}
			}
			
		});
	}
}
