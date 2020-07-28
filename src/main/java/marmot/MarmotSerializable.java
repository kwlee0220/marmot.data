package marmot;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface MarmotSerializable {
	public RecordSchema getRecordSchema();
	
	public void copyToRecord(Record output);
//	public static T fromRecord(Record input);
	
	public default Record toRecord() {
		Record record = DefaultRecord.of(getRecordSchema());
		copyToRecord(record);
		return record;
	}
}