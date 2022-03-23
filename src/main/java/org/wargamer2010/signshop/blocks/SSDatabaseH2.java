package org.wargamer2010.signshop.blocks;

import org.wargamer2010.signshop.SignShop;

import java.io.File;
import java.sql.*;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;

public class SSDatabaseH2 { // H2 Database interface
    //private static final String downloadURL = "http://cloud.github.com/downloads/wargamer/SignShop/";
    // ^^^ What is this used for? Doesn't seem to be used when using H2 JDBC drivers
    private static Driver driver = null;
    private Connection conn = null;
    private String filename;

    public SSDatabaseH2(final String pFilename){
        filename = pFilename;

        if(driver == null)
            loadLib();

        checkLegacy();
        if(!open())
            SignShop.log("Connection to: " + filename + " could not be established", Level.WARNING);
    }

    public final void loadLib() {
        try {
            Class.forName("org.h2.Driver");
        } catch(ClassNotFoundException ex) {
            SignShop.log("Could not find JDBC class in Bukkit JAR, please report this issue with details at http://tiny.cc/signshop", Level.SEVERE);
            return;
        }
        driver = new org.h2.Driver();
    }

    private void checkLegacy() { // this is duplicated, can we avoid this?
        String dbdirname = "db";
        File dbdir = new File(SignShop.getInstance().getDataFolder(), dbdirname);
        if(!dbdir.exists() && !dbdir.mkdirs()) {
            SignShop.log("Could not create db directory in plugin folder. Will use old path (plugins/SignShop) in stead of (plugins/SignShop/ " + dbdirname + ").", Level.WARNING);
            return;
        }
        File olddb = new File(SignShop.getInstance().getDataFolder(), filename);
        File newdb = new File(SignShop.getInstance().getDataFolder(), (dbdirname + File.separator + filename));
        if(olddb.exists()) {
            if(!newdb.exists() && !olddb.renameTo(newdb)) {
                SignShop.log("Could not move " + filename + " to (plugins/SignShop/ " + dbdirname + ") directory. Please move the file manually. Will use old path for now.", Level.WARNING);
                return;
            }
        }

        filename = (dbdirname + File.separator + filename);
    }

    public final boolean open() {
        if(driver == null)
            return false;
        try {
            File DBFile = new File(SignShop.getInstance().getDataFolder(), filename);
            conn = driver.connect("jdbc:h2:file:./" + DBFile.getPath()+";MODE=MySQL", new Properties());
        } catch (SQLException ignored) {
        }
        return (conn != null);
    }

    public void close() {
        if (conn == null || driver == null)
            return;
        try {
            conn.close();
        } catch (SQLException ignored) {
        }
    }

    public Boolean tableExists(String tablename) {
        try {
            Map<Integer, Object> pars = new LinkedHashMap<>();
            ResultSet set = (ResultSet)runH2Statement("SELECT * FROM " + tablename, pars, true);
            // Is doing SELECT * FROM + tablename an SQL Injection vulnerability?
            if(set != null && set.next()) {
                set.close();
                return true;
            }
        } catch (SQLException ignored) {
        }
        return false;
    }

    public boolean columnExists(String needle){
        ResultSet result = (ResultSet)runH2Statement("PRAGMA table_info(Book);", null, true);
        if(result == null)
            return false;
        try {
            do {
                String columnName = result.getString("name");
                if(columnName.equalsIgnoreCase(needle))
                    return true;
            } while(result.next());
        } catch (SQLException ex) {
            SignShop.log("Failed to check for column existence on Book table because: " + ex.getMessage(), Level.WARNING);
        } finally {
            close();
        }
        return false;
    }

    public Object runH2Statement(String Query, Map<Integer, Object> params, Boolean expectingResult) {
        try {
            if(conn == null) {
                SignShop.log("Query: " + Query + " could not be run because the connection to: " + filename + " could not be established", Level.WARNING);
                return null;
            }
            PreparedStatement st = conn.prepareStatement(Query, PreparedStatement.RETURN_GENERATED_KEYS);
            SignShop.log(st.toString(),Level.WARNING);

            if(params != null && !params.isEmpty()) {
                for(Map.Entry<Integer, Object> param : params.entrySet()) {
                    if(param.getValue().getClass().equals(int.class) || param.getValue().getClass().equals(Integer.class)) {
                        st.setInt(param.getKey(), ((Integer)param.getValue()));
                    } else if(param.getValue().getClass().equals(String.class)) {
                        st.setString(param.getKey(), ((String)param.getValue()));
                    }
                }
            }
            if(expectingResult) {
                SignShop.log("from expectingresult check: " + st,Level.WARNING); // testing
                return st.executeQuery();
            } else {
                int result = st.executeUpdate();
                ResultSet genKeys = st.getGeneratedKeys();
                if(genKeys == null)
                    return result;
                else {
                    while(genKeys.next()){ // move cursor forward
                        SignShop.log("GENKEY from runStatement: " + genKeys.getString(1), Level.INFO);
                    }
                    try {
                        // RowID is similar to the value called on from last_insert_rowid()
                        return genKeys.getInt("RowID");
                    } catch(SQLException ex) {
                        SignShop.log("Query: " + Query + " threw exception: " + ex.getMessage(), Level.WARNING);
                        SignShop.log(st.toString(),Level.WARNING); // testing
                        return result;
                    }
                }
            }
        } catch(SQLException ex) {
            SignShop.log("Outer SQLException - Query: " + Query + " threw exception: " + ex.getMessage(), Level.WARNING);
            return null;
        }
    }
}