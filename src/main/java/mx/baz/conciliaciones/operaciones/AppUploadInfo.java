package mx.baz.conciliaciones.operaciones;

import mx.baz.conciliaciones.operaciones.configs.GlobalConfig;
import mx.baz.conciliaciones.operaciones.dao.Impl.UploadInfoDaoImpl;
import mx.baz.conciliaciones.operaciones.dao.serviceImpl.ServiceImpl;
import mx.baz.conciliaciones.operaciones.dto.PathInfo;
import mx.baz.conciliaciones.operaciones.utils.ConexionParametrizada;
import mx.baz.conciliaciones.operaciones.utils.ConnectionManager;
import mx.baz.conciliaciones.operaciones.utils.FSConn;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

public class AppUploadInfo {
    private static final Logger logger = LogManager.getLogger(AppUploadInfo.class);


    public static void main(String[] args) throws Exception {
        ServiceImpl impl = new ServiceImpl();
        ConexionParametrizada conexion = impl.loadMySQLConfig();
        GlobalConfig messages = new GlobalConfig("messages");
        logger.info(messages.getProperty("start_process"));
        try (Connection con = ConnectionManager.getConnection(conexion)) {
            logger.info(messages.getProperty("db_connection_success"));
            UploadInfoDaoImpl dao = new UploadInfoDaoImpl();
            PathInfo connInfo = dao.getDirectory(con);
            logger.info("Route file: " + connInfo.toString());
            if (connInfo != null) {
                FSConn fsconn = new FSConn();
                fsconn.setRoute(connInfo.getIp(), connInfo.getPath(), connInfo.getFile(), connInfo.getDomain(), connInfo
                        .getUser(), connInfo.getPassword());
                long startTime = System.currentTimeMillis();
                String finalMessage = fsconn.uploadInfoToDB(con, conexion) ? messages.getProperty("ok_status") : messages.getProperty("error_status");
                logger.info(finalMessage);
                long endTime = System.currentTimeMillis();
                long elapsedTimeMillis = endTime - startTime;
                logger.info(messages.getProperty("timer_excecution"), Long.valueOf(TimeUnit.MILLISECONDS.toSeconds(elapsedTimeMillis)));
                logger.info(messages.getProperty("inserted_ok"));
            }
        } catch (SQLException e) {
            logger.error(messages.getProperty("bad_coneccion"), e);
        } catch (Exception e) {
            logger.error(messages.getProperty("main_error"), e);
        }
    }

}
