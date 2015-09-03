package oracle.ucp.test;

import oracle.ucp.jdbc.JDBCConnectionPoolStatistics;
import oracle.ucp.jdbc.PoolDataSourceFactory;
import oracle.ucp.jdbc.PoolXADataSource;
import oracle.ucp.jdbc.oracle.OracleJDBCConnectionPoolStatistics;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Martin Simka
 */
public class Main {
    private static final Logger logger = Logger.getLogger(Main.class.getName());

    private static final String JDBC_URL = "jdbc:oracle:thin:@(DESCRIPTION=(LOAD_BALANCE=on)(ADDRESS=(PROTOCOL=TCP)(HOST=vmg72.mw.lab.eng.bos.redhat.com)(PORT=1521))(ADDRESS=(PROTOCOL=TCP)(HOST=vmg73.mw.lab.eng.bos.redhat.com)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=qaorap)))";

    public static void main(String[] args) throws Exception {
//        Logger oracleLogger = Logger.getLogger("oracle.jdbc");
//        oracleLogger.setLevel(Level.ALL);
//        FileHandler handler = new FileHandler("log.txt", false);
//        handler.setFormatter(new SimpleFormatter());
//        handler.setLevel(Level.ALL);
//        logger.addHandler(handler);
//        oracleLogger.addHandler(handler);

//        Properties props = new Properties();
//        props.setProperty();
//        props.setProperty();
//        props.setProperty();

        Connection conn = null;
        try {
            PoolXADataSource pds = PoolDataSourceFactory.getPoolXADataSource();

            pds.setConnectionPoolName("pool1");
            pds.setFastConnectionFailoverEnabled(true);
            pds.setONSConfiguration("nodes=vmg72.mw.lab.eng.bos.redhat.com:5000,vmg73.mw.lab.eng.bos.redhat.com:5000");
            pds.setConnectionFactoryClassName("oracle.jdbc.xa.client.OracleXADataSource");
            pds.setMinPoolSize(0);
            pds.setMaxPoolSize(10);
            pds.setInactiveConnectionTimeout(0);
            pds.setTimeToLiveConnectionTimeout(0);
            pds.setAbandonedConnectionTimeout(0);
            pds.setConnectionWaitTimeout(0);
            pds.setPropertyCycle(900);
            pds.setValidateConnectionOnBorrow(true);

            pds.setUser("ucp01");
            pds.setPassword("ucp01");
            pds.setURL(JDBC_URL);

            conn = pds.getXAConnection().getConnection();

            while (true) {
//                printStatistics(pds.getStatistics());
                determineCurrentNode(conn);
                printUserName(conn);
                Thread.sleep(5000);
            }
        } catch (Throwable t) {
//            logger.log(Level.SEVERE, "", t);
            t.printStackTrace();
        } finally {
            if (conn != null)
                conn.close();
        }

    }

    private static void printStatistics(JDBCConnectionPoolStatistics stats) {
        String fcfInfo = ((OracleJDBCConnectionPoolStatistics)stats).
                getFCFProcessingInfo();
        System.out.println("The FCF information: "+fcfInfo+".");
    }

    private static void printUserName(Connection conn) throws SQLException {
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery("select user from dual");
            while (rs.next()) {
                String user = rs.getString(1);
                System.out.println("User is: " + user);
                logger.log(Level.INFO, "User is: " + user);
            }
            rs.close();
        } finally {
            if (rs != null)
                rs.close();
            if (stmt != null)
                stmt.close();
        }
    }

    private static void determineCurrentNode(Connection conn) throws SQLException {
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery("select host_name from v$instance");
            while (rs.next()) {
                String user = rs.getString(1);
                System.out.println("Current node is: " + user);
                logger.log(Level.INFO, "Current node is: " + user);
            }
            rs.close();
        } finally {
            if (rs != null)
                rs.close();
            if (stmt != null)
                stmt.close();
        }
    }
}
