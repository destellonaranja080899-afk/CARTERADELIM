package mx.baz.conciliaciones.operaciones.configs;

import java.io.InputStream;
import java.util.Properties;

public class GlobalConfig {
    private static GlobalConfig instance;

    private Properties properties;

    public GlobalConfig(String propertiesFile) {
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(propertiesFile + ".properties")) {
            this.properties = new Properties();
            this.properties.load(input);
        } catch (Exception e) {
            throw new RuntimeException("Error cargando configuración", e);
        }
    }

    public static GlobalConfig getInstance(String propertiesFile) {
        if (instance == null)
            instance = new GlobalConfig(propertiesFile);
        return instance;
    }

    public String getProperty(String key) {
        return this.properties.getProperty(key);
    }

    public int getIntProperty(String key) {
        return Integer.parseInt(this.properties.getProperty(key));
    }

    public boolean getBooleanProperty(String key) {
        return Boolean.parseBoolean(this.properties.getProperty(key));
    }
}
