package marmot.remote.server;

import java.io.IOException;
import java.util.logging.LogManager;

import org.apache.log4j.PropertyConfigurator;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import utils.NetUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GrpcDataSetServiceServerMain {
	private static final int DEFAULT_PORT = 15685;
	
	private int m_port = -1;
	
	private GrpcDataSetServiceServerMain(int port) {
		m_port = port;
	}
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
//		LogManager.getRootLogger().setLevel(Level.OFF);
		
		LogManager.getLogManager().reset();
		
		int port = ( args.length > 0 ) ? Integer.parseInt(args[0]) : DEFAULT_PORT;
		GrpcDataSetServiceServerMain main = new GrpcDataSetServiceServerMain(port);
		main.start();
	}
	
	private void start() throws IOException, InterruptedException {
		GrpcDataSetServiceServant servant = new GrpcDataSetServiceServant();
		Server server = NettyServerBuilder.forPort(m_port)
											.addService(servant)
											.build();
		server.start();

		String host = NetUtils.getLocalHostAddress();
		System.out.printf("started: DataSetServer[host=%s, port=%d]%n", host, m_port);
		
		server.awaitTermination();
	}
}
