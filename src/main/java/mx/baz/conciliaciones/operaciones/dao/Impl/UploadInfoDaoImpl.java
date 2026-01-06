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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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


	public boolean uploadInfoMultithreaded(Connection ignored, String uri, HikariConfig config) {

		final int parentBatchSize = 12_000;
		final int workers = 3;

		BlockingQueue<List<String[]>> queue = new ArrayBlockingQueue<>(10);
		ExecutorService executor = Executors.newFixedThreadPool(workers + 1);
		HikariDataSource hikariDataSource = new HikariDataSource(config);

		logger.info("Threads activos: {}", workers + 1);
		logger.info(messages.getProperty("FILE_READ_START"));

		try {

			// =========================
			// PRODUCTOR (LECTOR)
			// =========================
			executor.execute(() -> {
				try (BufferedReader reader = new BufferedReader(new FileReader(uri), 16 * 1024 * 1024)) {

					List<String[]> chunk = new ArrayList<>(parentBatchSize);
					String line;

					while ((line = reader.readLine()) != null) {
						chunk.add(fastSplit(line, ';'));

						if (chunk.size() == parentBatchSize) {
							queue.put(chunk);   //  backpressure real
							chunk = new ArrayList<>(parentBatchSize);
						}
					}

					if (!chunk.isEmpty()) {
						queue.put(chunk);
					}

					// señal de fin para los workers
					for (int i = 0; i < workers; i++) {
						queue.put(Collections.emptyList());
					}

				} catch (Exception e) {
					logger.error(messages.getProperty("FILE_READ_ERROR"), e);
				}
			});

			// =========================
			// CONSUMIDORES (INSERTS)
			// =========================
			for (int i = 0; i < workers; i++) {
				executor.execute(() -> {
					try {
						while (true) {
							List<String[]> rows = queue.take();

							if (rows.isEmpty()) {
								break;
							}

							insertBatch(hikariDataSource, rows);
							rows.clear();
						}
					} catch (Exception e) {
						logger.error(messages.getProperty("error_insert_batch"), e);
					}
				});
			}

			executor.shutdown();
			executor.awaitTermination(2, TimeUnit.HOURS);

			logger.info(messages.getProperty("FILE_READ_COMPLETE"));
			return true;

		} catch (Exception e) {
			logger.error("Error general en carga", e);
			return false;
		} finally {
			hikariDataSource.close();
		}
	}
	private static String[] fastSplit(String line, char delimiter) {
		List<String> result = new ArrayList<>();
		int start = 0;

		for (int i = 0; i < line.length(); i++) {
			if (line.charAt(i) == delimiter) {
				result.add(line.substring(start, i));
				start = i + 1;
			}
		}
		result.add(line.substring(start));
		return result.toArray(new String[0]);
	}


	private int insertBatch(DataSource dataSource, List<String[]> rows) {
		String[] selectedColumns = messages.getProperty("columns").split(",");
		Map<String, Integer> fileIndex = FileColumnIndex.getIndexMap();

		int inserted = 0;
		String COLUMNS = messages.getProperty("columns");
		int expectedCols = countColumns(COLUMNS);
		String sql = "INSERT INTO " + messages.getProperty("table_and_schema") + " (" + COLUMNS + ") VALUES (" + String.join(",", Collections.nCopies(expectedCols, "?")) + ")";
		int yyyymmdd = Integer.parseInt(LocalDate.now().minusDays(1).format(DateTimeFormatter.BASIC_ISO_DATE));
		try(Connection con = dataSource.getConnection();
			PreparedStatement ps = con.prepareStatement(sql)) {
			con.setAutoCommit(false);
			int childBatchSize = 2000;
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
			logger.info("Rows",rows.size());
			if (counter % childBatchSize != 0)
				ps.executeBatch();
			con.commit();
			inserted = counter;
		} catch (SQLException e) {
			logger.error(messages.getProperty("error_insert_batch") + e.getMessage());
		}
		return inserted;
	}

	private int countColumns(String columns) {
		return (columns.split(",")).length;
	}
}
