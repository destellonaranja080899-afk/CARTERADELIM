package mx.baz.conciliaciones.operaciones.dao.Impl;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import mx.baz.conciliaciones.operaciones.configs.GlobalConfig;
import mx.baz.conciliaciones.operaciones.dao.IUploadInfoDao;
import mx.baz.conciliaciones.operaciones.dto.FileColumnIndex;
import mx.baz.conciliaciones.operaciones.dto.PathInfo;
import mx.baz.conciliaciones.operaciones.utils.ConexionParametrizada;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class UploadInfoDaoImpl implements IUploadInfoDao {
	private static final Logger logger = LogManager.getLogger(UploadInfoDaoImpl.class);

	private static GlobalConfig messages = new GlobalConfig("messages");

	private static final String CONFIG_PATH = messages.getProperty("application_route");

	public ConexionParametrizada loadMySQLConfig() throws IOException {
		Properties props = new Properties();
		try (FileInputStream input = new FileInputStream(CONFIG_PATH)) {
			logger.info(messages.getProperty("read_ok") + CONFIG_PATH);
			props.load(input);
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
		return new ConexionParametrizada(1, props

				.getProperty("jdbc.driver"), props
				.getProperty("jdbc.url"), props
				.getProperty("jdbc.user"), props
				.getProperty("jdbc.password"));
	}

	public PathInfo getDirectory(Connection con) {
		logger.info("Get data for path");
		ResultSet rs = null;
		try (CallableStatement stmt = con.prepareCall("{CALL bdsagoprod.fn_obtconreporteria(?)}")) {
			stmt.setString(1, "CARTERADELIM");
			rs = stmt.executeQuery();
			PathInfo item = new PathInfo();
			while (rs.next()) {
				item.setJob(rs.getString("FCNOMBREJOB"));
				item.setIp(rs.getString("FCIP"));
				item.setPath(rs.getString("FCPATH"));
				item.setFile(rs.getString("FCFILE"));
				item.setDomain(rs.getString("FCDOMAIN"));
				item.setUser(rs.getString("FCUSER"));
				item.setPassword(rs.getString("FCPASSWORD"));
			}
			return item;
		} catch (SQLException e) {
			logger.error("Could not get path info: {}", e.getMessage());
			return null;
		} finally {
			if (rs != null)
				try {
					rs.close();
				} catch (SQLException e) {
					logger.error("Could not close object ResultSet: {}", e.getMessage());
				}
		}
	}

	public boolean uploadInfoMultithreaded(Connection con, String uri, HikariConfig config) {
		boolean success = false;
		int parentBatchSize = 5000;
		int maxThreads = 5;
		ExecutorService executor = Executors.newFixedThreadPool(maxThreads);
		HikariDataSource hikariDataSource = new HikariDataSource(config);
		CompletionService<Integer> completionService = new ExecutorCompletionService<>(executor);
		int submittedTasks = 0;
		try (BufferedReader reader = new BufferedReader(new FileReader(uri))) {
			List<String[]> chunk = (List)new ArrayList<>(parentBatchSize);
			logger.info(messages.getProperty("FILE_READ_START"));
			String line;
			while ((line = reader.readLine()) != null) {
				String[] fields = line.split(";");
				chunk.add(fields);
				if (chunk.size() == parentBatchSize) {
					List<String[]> toInsert = (List)new ArrayList<>(chunk);
					completionService.submit(() -> Integer.valueOf(insertBatch((DataSource)hikariDataSource, toInsert)));
					submittedTasks++;
					chunk.clear();
				}
			}
			if (!chunk.isEmpty()) {
				List<String[]> toInsert = (List)new ArrayList<>(chunk);
				completionService.submit(() -> Integer.valueOf(insertBatch((DataSource)hikariDataSource, toInsert)));
				submittedTasks++;
			}
			int totalInserted = 0;
			for (int i = 0; i < submittedTasks; i++) {
				try {
					Future<Integer> f = completionService.take();
					totalInserted += ((Integer)f.get()).intValue();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				} catch (ExecutionException e) {
					logger.error(messages.getProperty("TASK_FAILED") + e.getMessage());
				}
			}
			logger.info(messages.getProperty("FILE_READ_COMPLETE"));
			logger.info(messages.getProperty("count_rows") + totalInserted);
			success = true;
		} catch (IOException e) {
			logger.error(messages.getProperty("FILE_READ_ERROR") + e.getMessage());
		} finally {
			executor.shutdown();
			try {
				executor.awaitTermination(2L, TimeUnit.HOURS);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
		return success;
	}

	private int insertBatch(DataSource dataSource, List<String[]> rows) {
		String[] selectedColumns = messages.getProperty("columns").split(",");
		Map<String, Integer> fileIndex = FileColumnIndex.getIndexMap();

		int inserted = 0;
		String COLUMNS = messages.getProperty("columns");
		int expectedCols = countColumns(COLUMNS);
		String sql = "INSERT INTO " + messages.getProperty("table_and_schema") + " (" + COLUMNS + ") VALUES (" + String.join(",", Collections.nCopies(expectedCols, "?")) + ")";

		try(Connection con = dataSource.getConnection();
			PreparedStatement ps = con.prepareStatement(sql)) {
			con.setAutoCommit(false);
			int childBatchSize = 500;
			int counter = 0;
			for (String[] row : rows) {
				int paramIndex = 1;
				for (String col : selectedColumns) {

					//logger.info("Insertando columnas en orden: " + Arrays.toString(selectedColumns));

					// Columnas calculadas (equivalente a withColumn)
					if ("fcusuario".equalsIgnoreCase(col)) {
						ps.setString(paramIndex++, "CREDITO");
						continue;
					}

					if ("fifecha".equalsIgnoreCase(col)) {
						int yyyymmdd = Integer.parseInt(
								LocalDate.now().minusDays(1)
										.format(DateTimeFormatter.BASIC_ISO_DATE)
						);
						ps.setInt(paramIndex++, yyyymmdd);
						continue;
					}

					// Columnas que vienen del archivo
					Integer idx = fileIndex.get(col);

					if (idx == null || idx >= row.length) {
						ps.setNull(paramIndex++, Types.VARCHAR);
					} else {
						String val = row[idx] != null ? row[idx].trim() : null;
						if (val == null || val.isEmpty()) {
							ps.setNull(paramIndex++, Types.VARCHAR);
						} else {
							ps.setString(paramIndex++, val);
						}

					}

				}
				ps.addBatch();
				counter++;
				if (counter % childBatchSize == 0) {
					ps.executeBatch();
					ps.clearBatch();
				}
			}
			if (counter % childBatchSize != 0)
				ps.executeBatch();
			con.commit();
			inserted = counter;
			logger.info(messages.getProperty("insert") + rows.size() + messages.getProperty("cols_pro"));
		} catch (SQLException e) {
			logger.error(messages.getProperty("error_insert_batch") + e.getMessage());
		}
		return inserted;
	}

	private int countColumns(String columns) {
		return (columns.split(",")).length;
	}
}
