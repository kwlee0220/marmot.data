package marmot.type;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Comparator;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LongType extends PrimitiveDataType implements IntegralDataType, Comparator<Long> {
	private static final long serialVersionUID = 1L;
	private static final LongType TYPE = new LongType();
	
	public static LongType get() {
		return TYPE;
	}
	
	private LongType() {
		super(TypeClass.LONG, Long.class);
	}

	@Override
	public Long newInstance() {
		return new Long(0);
	}
	
	@Override
	public Long parseInstance(String str) {
		str = str.trim();
		return (str.length() > 0) ? Long.parseLong(str) : null;
	}

	@Override
	public int compare(Long v1, Long v2) {
		return Long.compare(v1, v2);
	}

	@Override
	public void serialize(Object obj, ObjectOutputStream oos) throws IOException {
		if ( obj instanceof Long ) {
			oos.writeLong((Long)obj);
		}
		else {
			throw new IOException("input stream corrupted: not " + getClass());
		}
	}

	@Override
	public Object deserialize(ObjectInputStream ois) throws IOException {
		return ois.readLong();
	}
}
