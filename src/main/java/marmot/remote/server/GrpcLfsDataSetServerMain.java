package marmot.remote.server;

import java.io.File;
import java.io.IOException;
import java.util.logging.LogManager;

import org.apache.log4j.PropertyConfigurator;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import marmot.dataset.DataSetServer;
import marmot.dataset.LfsAvroDataSetServer;
import marmot.file.FileServer;
import utils.NetUtils;
import utils.jdbc.JdbcProcessor;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GrpcLfsDataSetServerMain {
	private static final int DEFAULT_PORT = 15685;
	private static final File STORE_ROOT = new File("/home/kwlee/tmp/datastore");
	private static final String JDBC_STR = "postgresql:node00:5432:marmot:urc2004:marmot";
	
	private final int m_port;
	private GrpcDataSetServiceServant m_servant;
	private GrpcFileServiceServant m_fileServant;
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
//		LogManager.getRootLogger().setLevel(Level.OFF);
		
		LogManager.getLogManager().reset();
		
		int port = ( args.length > 0 ) ? Integer.parseInt(args[0]) : DEFAULT_PORT;
		
		JdbcProcessor jdbc = JdbcProcessor.parseString(JDBC_STR);
		DataSetServer server = new LfsAvroDataSetServer(jdbc, STORE_ROOT);
		GrpcLfsDataSetServerMain main = new GrpcLfsDataSetServerMain(port, server, null);
		
		main.start();
	}
	
	private GrpcLfsDataSetServerMain(int port, DataSetServer server, FileServer fileServer) {
		m_port = port;
		m_servant = new GrpcDataSetServiceServant(server);
		m_fileServant = new GrpcFileServiceServant(fileServer);
	}
	
	private void start() throws IOException, InterruptedException {
		Server server = NettyServerBuilder.forPort(m_port)
											.addService(m_servant)
											.build();
		server.start();

		String host = NetUtils.getLocalHostAddress();
		System.out.printf("started: DataSetServer[host=%s, port=%d]%n", host, m_port);
		
		server.awaitTermination();
	}
}
