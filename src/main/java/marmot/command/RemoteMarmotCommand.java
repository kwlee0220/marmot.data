package marmot.command;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import javax.annotation.Nullable;

import org.apache.commons.text.StringSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import utils.PicocliCommand;
import utils.UsageHelp;
import utils.func.FOption;
import utils.io.IOUtils;

import marmot.remote.client.GrpcMarmotRuntimeProxy;
import picocli.CommandLine;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.RunLast;
import picocli.CommandLine.Spec;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class RemoteMarmotCommand implements PicocliCommand<GrpcMarmotRuntimeProxy> {
	private static final Logger s_logger = LoggerFactory.getLogger(RemoteMarmotCommand.class);
	
	@Spec protected CommandSpec m_spec;
	@Mixin private UsageHelp m_help;

	@Option(names={"--host", "-h"}, paramLabel="ip", description={"host ip"})
	@Nullable private String m_host = null;

	@Option(names={"--port", "-p"}, paramLabel="number", description={"host port number"})
	@Nullable private int m_port = -1;
	
	@Option(names={"-v"}, description={"verbose"})
	private boolean m_verbose = false;
	
	@Nullable private GrpcMarmotRuntimeProxy m_marmot;
	
	protected abstract void run(GrpcMarmotRuntimeProxy marmot) throws Exception;

	@SuppressWarnings("deprecation")
	public static final void run(RemoteMarmotCommand cmd, String... args) throws Exception {
		new CommandLine(cmd).parseWithHandler(new RunLast(), System.err, args);
	}
	
	@Override
	public void run() {
		GrpcMarmotRuntimeProxy marmot = null;
		try {
			configureLog4j();
			
			marmot = getInitialContext();
			run(marmot);
		}
		catch ( Exception e ) {
			System.err.printf("failed: %s%n%n", e);
			
			m_spec.commandLine().usage(System.out, Ansi.OFF);
		}
		finally {
			IOUtils.closeQuietly(marmot);
		}
	}

	private static final String DEFAULT_HOST = "localhost";
	private static final int DEFAULT_PORT = 15685;
	public GrpcMarmotRuntimeProxy getInitialContext() throws Exception {
		if ( m_marmot == null ) {
			String host = FOption.ofNullable(m_host)
							.orElse(FOption.ofNullable(System.getenv("MARMOT_GRPC_HOST")))
							.getOrElse(DEFAULT_HOST);
			int port = FOption.when(m_port > 0, m_port)
							.orElse(FOption.ofNullable(System.getenv("MARMOT_GRPC_PORT"))
											.map(Integer::parseInt))
							.getOrElse(DEFAULT_PORT);
			
			m_marmot = GrpcMarmotRuntimeProxy.connect(host, port);
		}
		
		return m_marmot;
	}

	private static final String ENVVAR_HOME = "MARMOT_HOME";
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
		if ( s_logger.isDebugEnabled() ) {
			s_logger.debug("use log4j.properties from {}", propsFile);
		}
	}
}