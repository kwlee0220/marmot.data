package marmot.command;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import javax.annotation.Nullable;

import org.apache.commons.text.StringSubstitutor;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import marmot.MarmotLfsServer;
import marmot.dataset.LfsAvroDataSetServer;
import picocli.CommandLine;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.RunLast;
import picocli.CommandLine.Spec;
import utils.PicocliCommand;
import utils.UsageHelp;
import utils.Utilities;
import utils.func.FOption;
import utils.jdbc.JdbcProcessor;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class MarmotLocalFsCommand implements PicocliCommand<MarmotLfsServer> {
	private static final Logger s_logger = LoggerFactory.getLogger(MarmotLocalFsCommand.class);
	private static final String ENVVAR_HOME = "MARMOT_HOME";
	private static final String CATALOG_JDBC = "h2_local::0:sa::~/catalog";
	private static final File DATASET_STORE_ROOT = new File(Utilities.getHomeDir(), "datasets"); 
	
	@Spec protected CommandSpec m_spec;
	@Mixin private UsageHelp m_help;

	@Option(names={"--root"}, paramLabel="path", description={"Dataset store root directory"})
	@Nullable private File m_rootDir = DATASET_STORE_ROOT;

	@Option(names={"--catalog"}, paramLabel="jdbc_str", description={"JDBC String"})
	@Nullable private String m_catalogJdbcStr = CATALOG_JDBC;
	
	@Option(names={"-v"}, description={"verbose"})
	protected boolean m_verbose = false;
	
	@Option(names={"-f", "--format"}, description={"format"})
	protected boolean m_format = false;
	
	@Nullable private MarmotLfsServer m_marmot;
	
	protected abstract void run(MarmotLfsServer marmot) throws Exception;

	@SuppressWarnings("deprecation")
	public static final void run(MarmotLocalFsCommand cmd, String... args) throws Exception {
		new CommandLine(cmd).parseWithHandler(new RunLast(), System.err, args);
	}
	
	@Override
	public void run() {
		try {
			configureLog4j();
			
			MarmotLfsServer marmot = getInitialContext();
			run(marmot);
		}
		catch ( Exception e ) {
			System.err.printf("failed: %s%n%n", e);
			
			m_spec.commandLine().usage(System.out, Ansi.OFF);
		}
	}
	
	public MarmotLfsServer getInitialContext() throws Exception {
		if ( m_marmot == null ) {
			if ( m_format ) {
				JdbcProcessor jdbc = JdbcProcessor.parseString(m_catalogJdbcStr);
				LfsAvroDataSetServer.format(jdbc, m_rootDir);
			}
			
			m_marmot = new MarmotLfsServer(m_catalogJdbcStr, m_rootDir);
		}
		
		return m_marmot;
	}

	@Override
	public void configureLog4j() throws IOException {
		String homeDir = FOption.ofNullable(System.getenv(ENVVAR_HOME))
								.getOrElse(() -> System.getProperty("user.dir"));
		File propsFile = new File(homeDir, "log4j.properties");
		if ( m_verbose ) {
			System.out.printf("use log4j.properties: file=%s%n", propsFile);
		}
		
		Properties props = new Properties();
		try ( InputStream is = new FileInputStream(propsFile) ) {
			props.load(is);
		}
		
		Map<String,String> bindings = Maps.newHashMap();
		bindings.put("marmot.home", propsFile.getParentFile().toString());

		String rfFile = props.getProperty("log4j.appender.rfout.File");
		rfFile = StringSubstitutor.replace(rfFile, bindings);
		props.setProperty("log4j.appender.rfout.File", rfFile);
		PropertyConfigurator.configure(props);
		if ( s_logger.isDebugEnabled() ) {
			s_logger.debug("use log4j.properties from {}", propsFile);
		}
	}
}