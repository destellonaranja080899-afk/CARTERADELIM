package mx.baz.conciliaciones.operaciones.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionManager {

	private ConnectionManager() {
		super();
	}

	private static final Logger logger = LogManager.getLogger(ConnectionManager.class);
	// Desarrollo
	/*
	public static final String URL_AWS = "jdbc:mysql://rds-aurora-dev.caynatckvznu.us-east-1.rds.amazonaws.com:3306/sagobd";
	public static final String USER_AWS = "admin";
	public static final String PASSWD_AWS = "20204dm1N$Ag0DBMi$Q1";
	// Produccion
	*/
	public static final String URL_AWS
	="jdbc:mysql://db-sago-prod2-cluster.cluster-cekeq8einymu.us-east-1.rds.amazonaws.com:3307/bdsagoprod";
	public static final String USER_AWS = "USRWRITEMON";
	public static final String PASSWD_AWS = "Wr1t3M0n";

	public static Connection getAWSConection() {
		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
			return DriverManager.getConnection(URL_AWS, USER_AWS, PASSWD_AWS);
		}
		catch (ClassNotFoundException | SQLException ex) {
			logger.error("Could not establish connection to AWS: ", ex);
		}
		return null;
	}
	public static Connection getConnection(ConexionParametrizada config) throws Exception {
		Class.forName(config.getDriver());
		return DriverManager.getConnection(config.getUrl(), config.getUsuario(), config.getPassword());
	}
}
