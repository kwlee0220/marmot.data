package marmot.type;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.LocalTime;

import utils.LocalTimes;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TimeType extends PrimitiveDataType implements ComparableDataType {
	private static final long serialVersionUID = 1L;
	private static final TimeType TYPE = new TimeType();
	
	public static TimeType get() {
		return TYPE;
	}
	
	private TimeType() {
		super(TypeClass.TIME, LocalTime.class);
	}

	@Override
	public LocalTime newInstance() {
		return LocalTime.now();
	}
	
	@Override
	public LocalTime parseInstance(String str) {
		str = str.trim();
		return (str.length() > 0) ? LocalTime.parse(str) : null;
	}

	@Override
	public void serialize(Object obj, ObjectOutputStream oos) throws IOException {
		if ( obj instanceof LocalTime ) {
			LocalTime lt = (LocalTime)obj;
			oos.writeLong(LocalTimes.toMillisOfDay(lt));
		}
		else {
			throw new IOException("input stream corrupted: not " + getClass());
		}
	}

	@Override
	public Object deserialize(ObjectInputStream ois) throws IOException {
		long millis = ois.readLong();
		return LocalTimes.fromMillisOfDay(millis);
	}
}
