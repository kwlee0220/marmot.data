package marmot.type;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Date;
import java.time.LocalDate;

import utils.LocalDates;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DateType extends PrimitiveDataType implements ComparableDataType {
	private static final long serialVersionUID = 1L;
	private static final DateType TYPE = new DateType();
	
	public static DateType get() {
		return TYPE;
	}
	
	private DateType() {
		super(TypeClass.DATE, Date.class);
	}

	@Override
	public Date newInstance() {
		return new Date(new java.util.Date().getTime());
	}
	
	@Override
	public Date parseInstance(String str) {
		str = str.trim();
		return (str.length() > 0) ? Date.valueOf(str) : null;
	}

	@Override
	public void serialize(Object obj, ObjectOutputStream oos) throws IOException {
		if ( obj instanceof LocalDate ) {
			LocalDate ldt = (LocalDate)obj;
			oos.writeLong(LocalDates.toEpochMillis(ldt));
		}
		else {
			throw new IOException("input stream corrupted: not " + getClass());
		}
	}

	@Override
	public Object deserialize(ObjectInputStream ois) throws IOException {
		long epoch = ois.readLong();
		return LocalDates.fromEpochMillis(epoch);
	}
}
