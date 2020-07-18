package marmot.csv;

import java.nio.charset.Charset;

import utils.UnitUtils;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class StoreAsCsvOptions /*implements PBSerializable<StoreAsCsvOptionsProto>*/ {
	private final CsvOptions m_csvOptions;
	private final FOption<Long> m_blockSize;
	private final FOption<String> m_compressionCodecName;
	
	private StoreAsCsvOptions(CsvOptions csvOpts, FOption<Long> blockSize,
								FOption<String> codecName) {
		m_csvOptions = csvOpts;
		m_blockSize = blockSize;
		m_compressionCodecName = codecName;
	}
	
	public static StoreAsCsvOptions DEFAULT() {
		return new StoreAsCsvOptions(CsvOptions.DEFAULT(), FOption.empty(), FOption.empty());
	}
	
	public static StoreAsCsvOptions DEFAULT(char delim) {
		return new StoreAsCsvOptions(CsvOptions.DEFAULT(delim), FOption.empty(), FOption.empty());
	}
	
	public CsvOptions getCsvOptions() {
		return m_csvOptions;
	}
	
	public char delimiter() {
		return m_csvOptions.delimiter();
	}

	public StoreAsCsvOptions delimiter(char delim) {
		return new StoreAsCsvOptions(m_csvOptions.delimiter(delim), m_blockSize,
									m_compressionCodecName);
	}
	
	public FOption<Character> quote() {
		return m_csvOptions.quote();
	}

	public StoreAsCsvOptions quote(char quote) {
		return new StoreAsCsvOptions(m_csvOptions.quote(quote), m_blockSize,
									m_compressionCodecName);
	}
	
	public FOption<Character> escape() {
		return m_csvOptions.escape();
	}
	
	public StoreAsCsvOptions escape(char escape) {
		return new StoreAsCsvOptions(m_csvOptions.escape(escape), m_blockSize,
									m_compressionCodecName);
	}
	
	public FOption<Charset> charset() {
		return m_csvOptions.charset();
	}

	public StoreAsCsvOptions charset(String charset) {
		return new StoreAsCsvOptions(m_csvOptions.charset(charset), m_blockSize,
									m_compressionCodecName);
	}

	public StoreAsCsvOptions charset(Charset charset) {
		return new StoreAsCsvOptions(m_csvOptions.charset(charset), m_blockSize,
									m_compressionCodecName);
	}
	
	public FOption<Boolean> headerFirst() {
		return m_csvOptions.headerFirst();
	}

	public StoreAsCsvOptions headerFirst(boolean flag) {
		return new StoreAsCsvOptions(m_csvOptions.headerFirst(flag), m_blockSize,
									m_compressionCodecName);
	}
	
	public FOption<Long> blockSize() {
		return m_blockSize;
	}

	public StoreAsCsvOptions blockSize(long blkSize) {
		return new StoreAsCsvOptions(m_csvOptions, FOption.of(blkSize),
									m_compressionCodecName);
	}

	public StoreAsCsvOptions blockSize(String blkSize) {
		return blockSize(UnitUtils.parseByteSize(blkSize));
	}
	
	public FOption<String> compressionCodecName() {
		return m_compressionCodecName;
	}

	public StoreAsCsvOptions compressionCodecName(String name) {
		return new StoreAsCsvOptions(m_csvOptions, m_blockSize,
									FOption.ofNullable(name));
	}
	
	@Override
	public String toString() {
		String headerFirst = m_csvOptions.headerFirst()
										.filter(f -> f)
										.map(f -> ", header")
										.getOrElse("");
		String csStr = !charset().toString().equalsIgnoreCase("utf-8")
						? String.format(", %s", charset().toString()) : "";
		return String.format("delim='%s'%s%s",
								delimiter(), headerFirst, csStr);
	}

/*
	public static StoreAsCsvOptions fromProto(StoreAsCsvOptionsProto proto) {
		CsvOptions csvOpts = CsvOptions.fromProto(proto.getCsvOptions());
		StoreAsCsvOptions opts = new StoreAsCsvOptions(csvOpts, FOption.empty(), FOption.empty());
		
		switch ( proto.getOptionalBlockSizeCase() ) {
			case BLOCK_SIZE:
				opts = opts.blockSize(proto.getBlockSize());
				break;
			default:
		}
		switch ( proto.getOptionalCompressionCodecNameCase() ) {
			case COMPRESSION_CODEC_NAME:
				opts = opts.compressionCodecName(proto.getCompressionCodecName());
				break;
			default:
		}
		
		return opts;
	}

	@Override
	public StoreAsCsvOptionsProto toProto() {
		StoreAsCsvOptionsProto.Builder builder = StoreAsCsvOptionsProto.newBuilder()
														.setCsvOptions(m_csvOptions.toProto());
		m_blockSize.ifPresent(builder::setBlockSize);
		m_compressionCodecName.ifPresent(builder::setCompressionCodecName);
		
		return builder.build();
	}
*/
}
