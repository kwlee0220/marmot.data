package marmot.type;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.LocalDateTime;

import utils.LocalDateTimes;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DateTimeType extends PrimitiveDataType implements ComparableDataType {
	private static final long serialVersionUID = 1L;
	private static final DateTimeType TYPE = new DateTimeType();
	
	public static DateTimeType get() {
		return TYPE;
	}
	
	private DateTimeType() {
		super(TypeClass.DATETIME, LocalDateTime.class);
	}

	@Override
	public LocalDateTime newInstance() {
		return LocalDateTime.now();
	}
	
	@Override
	public LocalDateTime parseInstance(String str) {
		str = str.trim();
		return (str.length() > 0) ? LocalDateTime.parse(str) : null;
	}

	@Override
	public void serialize(Object obj, ObjectOutputStream oos) throws IOException {
		if ( obj instanceof LocalDateTime ) {
			LocalDateTime ldt = (LocalDateTime)obj;
			oos.writeLong(LocalDateTimes.toEpochMillis(ldt));
		}
		else {
			throw new IOException("input stream corrupted: not " + getClass());
		}
	}

	@Override
	public Object deserialize(ObjectInputStream ois) throws IOException {
		long epoch = ois.readLong();
		return LocalDateTimes.fromEpochMillis(epoch);
	}
}