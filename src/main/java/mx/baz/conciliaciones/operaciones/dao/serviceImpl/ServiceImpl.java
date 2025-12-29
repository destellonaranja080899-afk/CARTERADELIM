package mx.baz.conciliaciones.operaciones.dao.serviceImpl;
import mx.baz.conciliaciones.operaciones.dao.Impl.UploadInfoDaoImpl;
import mx.baz.conciliaciones.operaciones.utils.ConexionParametrizada;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ServiceImpl {
    private static final Logger logger = LogManager.getLogger(ServiceImpl.class);

    static UploadInfoDaoImpl dao = new UploadInfoDaoImpl();

    public ConexionParametrizada loadMySQLConfig() throws Exception {
        return dao.loadMySQLConfig();
    }
}
