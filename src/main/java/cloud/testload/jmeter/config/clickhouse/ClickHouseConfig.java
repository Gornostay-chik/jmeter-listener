package main.java.cloud.testload.jmeter.config.clickhouse;

import org.apache.commons.lang3.StringUtils;
import org.apache.jmeter.visualizers.backend.BackendListenerContext;

/**
 * Configuration for ClickHouse.
 *
 * @author Alexander Babaev
 *
 */
public class ClickHouseConfig {

    /**
     * Default database name.
     */
    public static final String DEFAULT_DATABASE = "default";

    /**
     * Default url.
     */
    public static final String DEFAULT_CLICKHOUSE_URL = "localhost:8123";

    /**
     * Default port.
     */
    public static final String KEY_CLICKHOUSE_URL = "chUrl";

    /**
     * Config key for database name.
     */
    public static final String KEY_CLICKHOUSE_DATABASE = "chDatabase";

    /**
     * Config key for password.
     */
    public static final String KEY_CLICKHOUSE_PASSWORD = "chPassword";

    /**
     * Config key for user name.
     */
    public static final String KEY_CLICKHOUSE_USER = "chUser";

    /**
     * CH Host.
     */
    private String clickhouseURL;

    /**
     * CH User.
     */
    private String clickhouseUser;

    /**
     * CH Password.
     */
    private String clickhousePassword;

    /**
     * CH database name.
     */
    private String clickhouseDatabase;


    public ClickHouseConfig(BackendListenerContext context) {
        String clickhouseURL = context.getParameter(KEY_CLICKHOUSE_URL);

        if (StringUtils.isEmpty(clickhouseURL)) {
            throw new IllegalArgumentException(KEY_CLICKHOUSE_URL + "must not be empty!");
        }
        setClickhouseURL(clickhouseURL);

        String clickhouseUser = context.getParameter(KEY_CLICKHOUSE_USER);
        setClickhouseUser(clickhouseUser);

        String clickhousePassword = context.getParameter(KEY_CLICKHOUSE_PASSWORD);
        setClickhousePassword(clickhousePassword);

        String clickhouseDatabase = context.getParameter(KEY_CLICKHOUSE_DATABASE);
        if (StringUtils.isEmpty(clickhouseDatabase)) {
            throw new IllegalArgumentException(KEY_CLICKHOUSE_DATABASE + "must not be empty!");
        }
        setClickhouseDatabase(clickhouseDatabase);

    }

    public String getClickhouseURL() {
        return clickhouseURL;
    }

    public void setClickhouseURL(String clickhouseURL) {
        this.clickhouseURL = clickhouseURL;
    }

    public String getClickhouseUser() {
        return clickhouseUser;
    }

    public void setClickhouseUser(String clickhouseUser) {
        this.clickhouseUser = clickhouseUser;
    }

    public String getClickhousePassword() {
        return clickhousePassword;
    }

    public void setClickhousePassword(String clickhousePassword) {
        this.clickhousePassword = clickhousePassword;
    }

    public String getClickhouseDatabase() {
        return clickhouseDatabase;
    }

    public void setClickhouseDatabase(String clickhouseDatabase) {
        this.clickhouseDatabase = clickhouseDatabase;
    }
}
