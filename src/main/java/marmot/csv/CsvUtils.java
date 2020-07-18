package marmot.csv;

import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.Column;
import marmot.RecordSchema;
import marmot.type.DataType;
import utils.CSV;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CsvUtils {
	private static final Logger s_logger = LoggerFactory.getLogger(CsvUtils.class);
	
	private CsvUtils() {
		throw new AssertionError("should not be called: " + CsvUtils.class);
	}
	
	public static RecordSchema buildRecordSchema(String[] colSpecs) {
		return buildRecordSchema(Arrays.asList(colSpecs));
	}
	
	public static RecordSchema buildRecordSchema(List<String> colSpecs) {
		return FStream.from(colSpecs)
						.map(CsvUtils::parseColumnSpec)
						.foldLeft(RecordSchema.builder(), (b,c) -> b.addColumn(c))
						.build();
	}
	
	private static String adjustColumnName(String colName) {
		// marmot에서는 컬럼이름에 '.'이 들어가는 것을 허용하지 않기 때문에
		// '.' 문자를 '_' 문제로 치환시킨다.
		if ( colName.indexOf('.') > 0 )  {
			String replaced = colName.replaceAll("\\.", "_");
			s_logger.warn("column name replaced: '{}' -> '{}'", colName, replaced);
			colName = replaced;
		}
		// marmot에서는 컬럼이름에 공백문자가  들어가는 것을 허용하지 않기 때문에
		// 공백문자를 '_' 문제로 치환시킨다.
		if ( colName.indexOf(' ') > 0 )  {
			String replaced = colName.replaceAll(" ", "_");
			s_logger.warn("column name replaced: '{}' -> '{}'", colName, replaced);
			colName = replaced;
		}
		if ( colName.indexOf('(') > 0 )  {
			String replaced = colName.replaceAll("\\(", "_");
			s_logger.warn("column name replaced: '{}' -> '{}'", colName, replaced);
			colName = replaced;
		}
		if ( colName.indexOf(')') > 0 )  {
			String replaced = colName.replaceAll("\\)", "_");
			s_logger.warn("column name replaced: '{}' -> '{}'", colName, replaced);
			colName = replaced;
		}
		
		return colName;
	}
	
	private static Column parseColumnSpec(String spec) {
		List<String> parts = CSV.parseCsv(spec, ':').toList();
		
		String colName = adjustColumnName(parts.get(0));
		DataType colType = (parts.size() == 2)
							? DataType.parseDisplayName(parts.get(1).toUpperCase())
							: DataType.STRING;
		return new Column(colName, colType);
	}
}
