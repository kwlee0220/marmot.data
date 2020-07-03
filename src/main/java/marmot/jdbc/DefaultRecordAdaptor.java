package marmot.jdbc;

import marmot.RecordSchema;
import utils.Utilities;
import utils.jdbc.JdbcProcessor;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DefaultRecordAdaptor extends JdbcRecordAdaptor {
	public DefaultRecordAdaptor(JdbcProcessor jdbc, RecordSchema schema, GeometryFormat format) {
		super(jdbc, schema, format);
		
		Utilities.checkArgument(format != GeometryFormat.NATIVE,
							"default JdbcRecordAdaptor does not support 'NATIVE' geometry format");
	}
}
