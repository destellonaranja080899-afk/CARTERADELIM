package mx.baz.conciliaciones.operaciones.utils;

import com.zaxxer.hikari.HikariConfig;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import mx.baz.conciliaciones.operaciones.configs.GlobalConfig;
import mx.baz.conciliaciones.operaciones.dao.Impl.UploadInfoDaoImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FSConn {
	private static GlobalConfig messages = new GlobalConfig("messages");

	private static final Logger logger = LogManager.getLogger(FSConn.class);

	private static final String SEPARATOR = File.separator;

	private static final String ABC = messages.getProperty("abc");

	private String url;

	private String uri;

	private String ip;

	private String path;

	private String file;

	private String domain;

	private String user;

	private String password;

	private String domainAndUser;

	private String drive;

	public FSConn() {}

	public FSConn(String ip, String path, String file, String domain, String user, String password) {
		this.ip = ip;
		this.path = path;
		this.file = file;
		this.domain = domain;
		this.user = user;
		this.password = password;
		build();
	}

	public void setRoute(String ip, String path, String file, String domain, String user, String password) {
		this.ip = ip;
		this.path = path;
		this.file = file;
		this.domain = domain;
		this.user = user;
		this.password = password;
		build();
	}

	private void build() {
		LocalDate yesterday = LocalDate.now().minusDays(1L);
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyMMdd");
		String formattedDate = yesterday.format(formatter);
		this.file += formattedDate + "";
		if (this.path.contains("/"))
			this.path = this.path.replace("/", SEPARATOR);
		if (this.path.contains("\\"))
			this.path = this.path.replace("\\", SEPARATOR);
		if (this.path.startsWith(SEPARATOR))
			this.path = this.path.substring(1, this.path.length());
		if (this.path.endsWith(SEPARATOR))
			this.path = this.path.substring(0, this.path.length() - 1);
		this.url = SEPARATOR + SEPARATOR + this.ip + SEPARATOR + this.path;
		this.uri = SEPARATOR + SEPARATOR + this.ip + SEPARATOR + this.path + SEPARATOR + this.file;
		if (this.domain != null || this.user != null)
			this.domainAndUser = this.domain + SEPARATOR + this.user;
		logger.info("URI: {}", this.uri);
		logger.info("URN: {}", this.url);
		logger.info("IP: {}", this.ip);
		logger.info("Path: {}", this.path);
		logger.info("File: {}", this.file);
		logger.info("User and Domain: {}", this.domainAndUser);
	}

	private boolean connectToNetworkDrive() {
		boolean success = false;
		String command = "";
		char[] alphabet = ABC.toCharArray();
		for (char letter : alphabet) {
			this.drive = letter + ":";
			command = String.format("net use %s %s %s /user:%s", new Object[] { this.drive, this.url, this.password, this.domainAndUser });
			logger.info("Command: {}", command);
			success = exec(command);
			if (success)
				break;
		}
		if (success)
			logger.info("Success conection to network drive: {}", Boolean.valueOf(success));
		return success;
	}

	private boolean closeConnectionWithNetworkDrive() {
		String command = String.format("net use %s /delete", new Object[] { this.drive });
		logger.info("Command: {}", command);
		boolean success = exec(command);
		if (!success) {
			logger.info("Could not delete network dirve");
		} else {
			logger.info("Network driver closed");
		}
		return success;
	}

	public boolean uploadInfoToDB(Connection con, ConexionParametrizada connexion) {
		boolean success = false;
		try {
			if (!connectToNetworkDrive())
				throw new IOException(messages.getProperty("error_drive_con"));
			UploadInfoDaoImpl dao = new UploadInfoDaoImpl();
			HikariConfig config = new HikariConfig();
			config.setJdbcUrl(connexion.getUrl());
			config.setUsername(connexion.getUsuario());
			config.setPassword(connexion.getPassword());
			// config.setMaximumPoolSize(16);
			if (!dao.uploadInfoMultithreaded(con, this.uri, config))
				throw new SQLException(messages.getProperty("inserted_error"));
			success = true;
		} catch (IOException e) {
			logger.info(messages.getProperty("network_error"), e.getMessage());
		} catch (SQLException e) {
			logger.info(messages.getProperty("inserted_error_param"), e.getMessage());
		} finally {
			closeConnectionWithNetworkDrive();
		}
		return success;
	}

	private boolean exec(String comando) {
		boolean success = false;
		Runtime runtime = Runtime.getRuntime();
		InputStream in = null;
		try {
			String command = "cmd";
			Process exec = runtime.exec(command);
			PrintWriter stdin = new PrintWriter(exec.getOutputStream());
			stdin.println(comando);
			stdin.close();
			in = exec.getInputStream();
			int length = -1;
			byte[] buffer = new byte[1024];
			StringBuilder sb = new StringBuilder();
			while ((length = in.read(buffer)) != -1)
				sb.append(new String(buffer, 0, length, "GBK"));
			success = true;
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (in != null)
				try {
					in.close();
				} catch (IOException e) {
					logger.error("Could not close object InputSream: {}", e.getMessage());
				}
		}
		return success;
	}
}
