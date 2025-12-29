package mx.baz.conciliaciones.operaciones.dao;

import mx.baz.conciliaciones.operaciones.dto.PathInfo;
import com.zaxxer.hikari.HikariConfig;
import java.sql.Connection;

public interface IUploadInfoDao {
    /**
     *
     * @param con
     * @return
     */
    PathInfo getDirectory(Connection con);

    /**
     *
     * @param paramConnection
     * @param paramString
     * @param paramHikariConfig
     * @return
     */
    boolean uploadInfoMultithreaded(Connection paramConnection, String paramString, HikariConfig paramHikariConfig);
}
