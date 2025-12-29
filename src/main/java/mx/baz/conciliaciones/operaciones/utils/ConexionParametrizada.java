package mx.baz.conciliaciones.operaciones.utils;

public class ConexionParametrizada {
    private int tipoBD;

    private String driver;

    private String url;

    private String usuario;

    private String password;

    public ConexionParametrizada(int tipoBD, String driver, String url, String usuario, String password) {
        this.tipoBD = tipoBD;
        this.driver = driver;
        this.url = url;
        this.usuario = usuario;
        this.password = password;
    }

    public int getTipoBD() {
        return this.tipoBD;
    }

    public String getDriver() {
        return this.driver;
    }

    public String getUrl() {
        return this.url;
    }

    public String getUsuario() {
        return this.usuario;
    }

    public String getPassword() {
        return this.password;
    }

    public String toString() {
        return "ConexionParametrizada{tipoBD=" + this.tipoBD + ", driver='" + this.driver + '\'' + ", url='" + this.url + '\'' + ", usuario='" + this.usuario + '\'' + ", password='" + this.password + '\'' + '}';
    }
}
