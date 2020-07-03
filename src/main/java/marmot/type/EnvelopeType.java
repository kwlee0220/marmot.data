package marmot.type;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;

import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class EnvelopeType extends PrimitiveDataType {
	private static final long serialVersionUID = 1L;
	public static final Envelope EMPTY = new Envelope();
	private static final EnvelopeType TYPE = new EnvelopeType();
	
	public static EnvelopeType get() {
		return TYPE;
	}
	
	private EnvelopeType() {
		super(TypeClass.ENVELOPE, Envelope.class);
	}

	@Override
	public Envelope newInstance() {
		return new Envelope();
	}
	
	@Override
	public Envelope parseInstance(String str) {
		return parseEnvelope(str).getOrThrow(() -> new IllegalArgumentException("envelope_string=" + str));
	}
	
	@Override
	public String toInstanceString(Object instance) {
		return toString((Envelope)instance);
	}

	private static final String FPN = "(\\d+(\\.\\d*)?|(\\d+)?\\.\\d+)";
	private static final String PT = String.format("\\(\\s*%s\\s*,\\s*%s\\s*\\)", FPN, FPN);
	private static final String SZ = String.format("\\s*%s\\s*[xX]\\s*%s", FPN, FPN);
	private static final String ENVL =  String.format("\\s*%s\\s*:\\s*%s", PT, SZ);
	private static final Pattern PATTERN_ENVL = Pattern.compile(ENVL);
	public static FOption<Envelope> parseEnvelope(String expr) {
		Matcher matcher = PATTERN_ENVL.matcher(expr);
		if ( matcher.find() ) {
			FOption.empty();
		}
		
		double x = Double.parseDouble(matcher.group(1));
		double y = Double.parseDouble(matcher.group(4));
		Coordinate min = new Coordinate(x, y);
		
		double width = Double.parseDouble(matcher.group(7));
		double height = Double.parseDouble(matcher.group(10));
		Coordinate max = new Coordinate(x + width, y + height);
		
		return FOption.of(new Envelope(min, max));
	}
	
	public static String toString(Envelope envl) {
		double width = envl.getMaxX() - envl.getMinX();
		double height = envl.getMaxY() - envl.getMinY();
		return String.format("(%f,%f):%fx%f", envl.getMinX(), envl.getMinY(), width, height);
	}

	@Override
	public void serialize(Object obj, ObjectOutputStream oos) throws IOException {
		if ( obj instanceof Envelope ) {
			Envelope envl = (Envelope)obj;
			oos.writeDouble(envl.getMinX());
			oos.writeDouble(envl.getMaxX());
			oos.writeDouble(envl.getMinY());
			oos.writeDouble(envl.getMaxY());
		}
		else {
			throw new IOException("input stream corrupted: not " + getClass());
		}
	}

	@Override
	public Object deserialize(ObjectInputStream ois) throws IOException {
		return new Envelope(ois.readDouble(), ois.readDouble(), ois.readDouble(), ois.readDouble());
	}
}
