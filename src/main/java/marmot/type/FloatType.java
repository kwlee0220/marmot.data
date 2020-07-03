package marmot.type;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Comparator;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FloatType extends PrimitiveDataType implements NumericDataType, Comparator<Float> {
	private static final long serialVersionUID = 1L;
	private static final FloatType TYPE = new FloatType();
	
	public static FloatType get() {
		return TYPE;
	}
	
	private FloatType() {
		super(TypeClass.FLOAT, Float.class);
	}

	@Override
	public Float newInstance() {
		return new Float(0);
	}
	
	@Override
	public Float parseInstance(String str) {
		str = str.trim();
		return (str.length() > 0) ? Float.parseFloat(str) : null;
	}

	@Override
	public int compare(Float v1, Float v2) {
		return Float.compare(v1, v2);
	}

	@Override
	public void serialize(Object obj, ObjectOutputStream oos) throws IOException {
		if ( obj instanceof Float ) {
			oos.writeFloat((Float)obj);
		}
		else {
			throw new IOException("input stream corrupted: not " + getClass());
		}
	}

	@Override
	public Object deserialize(ObjectInputStream ois) throws IOException {
		return ois.readFloat();
	}
}
