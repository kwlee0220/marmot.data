package marmot.type;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Comparator;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DoubleType extends PrimitiveDataType implements NumericDataType, Comparator<Double> {
	private static final long serialVersionUID = 1L;
	private static final DoubleType TYPE = new DoubleType();
	
	public static DoubleType get() {
		return TYPE;
	}
	
	private DoubleType() {
		super(TypeClass.DOUBLE, Double.class);
	}

	@Override
	public Double newInstance() {
		return new Double(0);
	}
	
	@Override
	public Double parseInstance(String str) {
		str = str.trim();
		return (str.length() > 0) ? Double.parseDouble(str) : null;
	}

	@Override
	public int compare(Double v1, Double v2) {
		return Double.compare(v1, v2);
	}

	@Override
	public void serialize(Object obj, ObjectOutputStream oos) throws IOException {
		if ( obj instanceof Double ) {
			oos.writeDouble((Double)obj);
		}
		else {
			throw new IOException("input stream corrupted: not " + getClass());
		}
	}

	@Override
	public Object deserialize(ObjectInputStream ois) throws IOException {
		return ois.readDouble();
	}
}
