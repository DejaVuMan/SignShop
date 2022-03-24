
package org.wargamer2010.signshop.player;

import org.bukkit.plugin.Plugin;
import org.wargamer2010.signshop.SignShop;
import org.wargamer2010.signshop.blocks.SSDatabaseH2;
import org.wargamer2010.signshop.blocks.SSDatabaseSqlite;
import org.wargamer2010.signshop.configuration.SignShopConfig;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

public class PlayerMetadata {
    private static final String filename = "player.db";
    private final SignShopPlayer ssPlayer;
    private final Plugin plugin;

    public PlayerMetadata(SignShopPlayer pPlayer, Plugin pPlugin) {
        ssPlayer = pPlayer;
        plugin = pPlugin;
    }

    public static void init() {
        if(SignShopConfig.SqlDbTypeSelector.equals("SQLite")){
            SSDatabaseSqlite metadb = new SSDatabaseSqlite(filename);
            try {
                if(!metadb.tableExists("PlayerMeta"))
                    metadb.runSqliteStatement(
                            "CREATE TABLE PlayerMeta ( PlayerMetaID INTEGER, Playername TEXT NOT NULL, " +
                                    "Plugin TEXT NOT NULL, Metakey TEXT NOT NULL, Metavalue TEXT NOT NULL, PRIMARY KEY(PlayerMetaID) )", null, false);
            } finally {
                metadb.close();
            }
        } else {
            SSDatabaseH2 metadb = new SSDatabaseH2(filename);
            try {
                if(!metadb.tableExists("PlayerMeta"))
                    metadb.runH2Statement(
                            "CREATE TABLE PlayerMeta ( PlayerMetaID INTEGER NOT NULL AUTO_INCREMENT, Playername TEXT NOT NULL, " +
                                    "Plugin TEXT NOT NULL, Metakey TEXT NOT NULL, Metavalue TEXT NOT NULL, " +
                                    "RowID INTEGER NOT NULL AUTO_INCREMENT, PRIMARY KEY(PlayerMetaID) )", null, false);
            } finally {
                metadb.close();
            }
        }
    }

    public boolean hasMeta(String key) {
        return (getMetaValue(key) != null);
    }

    /**
     * Attempts to convert all player names to UUID where needed
     * To be Called a single time on plugin startup
     *
     * @param pPlugin Plugin
     */
    // Assignment is used in case of exception
    public static <T> void convertToUuid(Plugin pPlugin) {
        if (!PlayerIdentifier.GetUUIDSupport())
            return; // Legacy mode

        T metadb;

        Map<Integer, Object> params = new LinkedHashMap<>();
        params.put(1, pPlugin.getName());
        ToConvert lastAttempt = null;

        if(SignShopConfig.SqlDbTypeSelector.equals("SQLite")){
            metadb = (T) new SSDatabaseSqlite(filename);
        } else {
            metadb = (T) new SSDatabaseH2(filename);
        }

        try {
            ResultSet set;
            if(SignShopConfig.SqlDbTypeSelector.equals("SQLite")){
                set = (ResultSet) ((SSDatabaseSqlite)metadb).runSqliteStatement(
                        "SELECT Playername, Metakey, Metavalue FROM PlayerMeta WHERE Plugin = ?", params, true);
            } else {
                set = (ResultSet) ((SSDatabaseH2)metadb).runH2Statement(
                        "SELECT Playername, Metakey, Metavalue FROM PlayerMeta WHERE Plugin = ?", params, true);
            }
            if (set == null)
                return;
            List<ToConvert> toConverts = new LinkedList<>();
            while (set.next()) {
                String playername = set.getString("Playername");
                String metakey = set.getString("Metakey");
                String metavalue = set.getString("Metavalue");
                if (playername == null || metakey == null)
                    continue;
                SignShopPlayer player = PlayerIdentifier.getPlayerFromString(playername);
                if (player == null)
                    continue;
                // Adding a NPE check to solve an issue where SignShop is failing to load.
                // Presumably the DB file is missing some necessary info, but in the sample
                // case UUID conversion happened long ago, so it does not need to happen again.
                if (player.GetIdentifier() == null)
                    continue;
                String id = player.GetIdentifier().toString();
                if (!playername.equalsIgnoreCase(id))
                    toConverts.add(new ToConvert(playername, id, metakey, metavalue));
            }
            set.close();

            if (toConverts.size() > 0) {
                SignShop.log("Starting conversion from Player name to UUID for PlayerMeta table. Please be patient and don't interrupt the process.", Level.INFO);
            }

            for (ToConvert convert : toConverts) {
                lastAttempt = convert;

                params.clear();
                params.put(1, pPlugin.getName());
                params.put(2, convert.playerName);
                params.put(3, convert.metakey);
                if(SignShopConfig.SqlDbTypeSelector.equals("SQLite")) {
                    ((SSDatabaseSqlite)metadb).runSqliteStatement(
                            "DELETE FROM PlayerMeta WHERE Plugin = ? AND Playername = ? AND Metakey = ?", params, false);
                } else {
                    ((SSDatabaseH2)metadb).runH2Statement(
                            "DELETE FROM PlayerMeta WHERE Plugin = ? AND Playername = ? AND Metakey = ?", params, false);
                }

                params.clear();
                params.put(1, pPlugin.getName());
                params.put(2, convert.newId);
                params.put(3, convert.metakey);
                params.put(4, convert.metavalue);
                if(SignShopConfig.SqlDbTypeSelector.equals("SQLite")) {
                    ((SSDatabaseSqlite)metadb).runSqliteStatement("INSERT INTO PlayerMeta(Plugin, Playername, Metakey, Metavalue) VALUES (?, ?, ?, ?)", params, false);
                } else {
                    ((SSDatabaseH2)metadb).runH2Statement("INSERT INTO PlayerMeta(Plugin, Playername, Metakey, Metavalue) VALUES (?, ?, ?, ?)", params, false);
                }
            }
        } catch (SQLException ex) {
            SignShop.log("Failed to convert Player names to UUID in PlayerMeta table because: " + ex.getMessage(), Level.WARNING);
            if (lastAttempt != null)
                SignShop.log(String.format("Failed conversion at meta for player '%s' with metakey '%s'", lastAttempt.playerName, lastAttempt.metakey), Level.WARNING);
        } finally {
            if(SignShopConfig.SqlDbTypeSelector.equals("SQLite")){
                ((SSDatabaseSqlite)metadb).close();
            } else {
                ((SSDatabaseH2)metadb).close();
            }
        }
    }

    public <T> String getMetaValue(String key) {
        Map<Integer, Object> params = new LinkedHashMap<>();
        params.put(1, plugin.getName());
        params.put(2, ssPlayer.GetIdentifier().toString());
        params.put(3, key);

        T metadb;
        if(SignShopConfig.SqlDbTypeSelector.equals("SQLite")){
            metadb = (T) new SSDatabaseSqlite(filename);
        } else {
            metadb = (T) new SSDatabaseH2(filename);
        }

        try {
            ResultSet set;
            if(SignShopConfig.SqlDbTypeSelector.equals("SQLite")) {
                set = (ResultSet) ((SSDatabaseSqlite) metadb).runSqliteStatement(
                        "SELECT Metavalue FROM PlayerMeta WHERE Plugin = ? AND Playername = ? AND Metakey = ?", params, true);
            } else {
                set = (ResultSet) ((SSDatabaseH2) metadb).runH2Statement(
                        "SELECT Metavalue FROM PlayerMeta WHERE Plugin = ? AND Playername = ? AND Metakey = ?", params, true);
            }
            if(set == null)
                return null;
            if(set.next())
                return set.getString("Metavalue");
            else
                return null;
        } catch(SQLException ex) {
            return null;
        } finally {
            if(SignShopConfig.SqlDbTypeSelector.equals("SQLite")){
                ((SSDatabaseSqlite)metadb).close();
            } else {
                ((SSDatabaseH2)metadb).close();
            }
        }
    }

    public <T> boolean setMetavalue(String key, String value) {
        if(getMetaValue(key) != null) {
            return updateMeta(key, value);
        }
        T metadb;
        if(SignShopConfig.SqlDbTypeSelector.equals("SQLite")){
            metadb = (T) new SSDatabaseSqlite(filename);
        } else {
            metadb = (T) new SSDatabaseH2(filename);
        }
        try {
            Map<Integer, Object> params = new LinkedHashMap<>();
            params.put(1, plugin.getName());
            params.put(2, ssPlayer.GetIdentifier().toString());
            params.put(3, key);
            params.put(4, value);
            if(SignShopConfig.SqlDbTypeSelector.equals("SQLite")) {
                return (((SSDatabaseSqlite)metadb).runSqliteStatement(
                        "INSERT INTO PlayerMeta(Plugin, Playername, Metakey, Metavalue) VALUES (?, ?, ?, ?)", params, false) != null);
            } else {
                return (((SSDatabaseH2)metadb).runH2Statement(
                        "INSERT INTO PlayerMeta(Plugin, Playername, Metakey, Metavalue) VALUES (?, ?, ?, ?)", params, false) != null);
            }
        } finally {
            if(SignShopConfig.SqlDbTypeSelector.equals("SQLite")) {
                ((SSDatabaseSqlite)metadb).close();
            } else {
                ((SSDatabaseH2)metadb).close();
            }
        }
    }

    public <T> boolean updateMeta(String key, String value) {
        if(getMetaValue(key) != null) {
            return updateMeta(key, value);
        }
        T metadb;
        if(SignShopConfig.SqlDbTypeSelector.equals("SQLite")){
            metadb = (T) new SSDatabaseSqlite(filename);
        } else {
            metadb = (T) new SSDatabaseH2(filename);
        }
        try {
            Map<Integer, Object> params = new LinkedHashMap<>();
            params.put(1, plugin.getName());
            params.put(2, ssPlayer.GetIdentifier().toString());
            params.put(3, key);
            params.put(4, value);
            if(SignShopConfig.SqlDbTypeSelector.equals("SQLite")) {
                return (((SSDatabaseSqlite)metadb).runSqliteStatement(
                "UPDATE PlayerMeta SET Metavalue = ? WHERE Plugin = ? AND Playername = ? AND Metakey = ?", params, false) != null);
            } else {
                return (((SSDatabaseH2)metadb).runH2Statement(
                        "UPDATE PlayerMeta SET Metavalue = ? WHERE Plugin = ? AND Playername = ? AND Metakey = ?", params, false) != null);
            }
        } finally {
            if(SignShopConfig.SqlDbTypeSelector.equals("SQLite")) {
                ((SSDatabaseSqlite)metadb).close();
            } else {
                ((SSDatabaseH2)metadb).close();
            }
        }
    }

    public <T> boolean removeMeta(String key) {
        T metadb;
        if(SignShopConfig.SqlDbTypeSelector.equals("SQLite")){
            metadb = (T) new SSDatabaseSqlite(filename);
        } else {
            metadb = (T) new SSDatabaseH2(filename);
        }
        try {
            Map<Integer, Object> params = new LinkedHashMap<>();
            params.put(1, plugin.getName());
            params.put(2, ssPlayer.GetIdentifier().toString());
            params.put(3, key);
            if(SignShopConfig.SqlDbTypeSelector.equals("SQLite")){
                return (((SSDatabaseSqlite)metadb).runSqliteStatement(
                        "DELETE FROM PlayerMeta WHERE Plugin = ? AND Playername = ? AND Metakey = ?", params, false) != null);
            } else {
                return (((SSDatabaseH2)metadb).runH2Statement(
                        "DELETE FROM PlayerMeta WHERE Plugin = ? AND Playername = ? AND Metakey = ?", params, false) != null);
            }
        } finally {
            if(SignShopConfig.SqlDbTypeSelector.equals("SQLite")) {
                ((SSDatabaseSqlite)metadb).close();
            } else {
                ((SSDatabaseH2)metadb).close();
            }
        }
    }

    public <T> void removeMetakeyLike(String key) {
        T metadb;
        if(SignShopConfig.SqlDbTypeSelector.equals("SQLite")){
            metadb = (T) new SSDatabaseSqlite(filename);
        } else {
            metadb = (T) new SSDatabaseH2(filename);
        }
        try {
            Map<Integer, Object> params = new LinkedHashMap<>();
            params.put(1, plugin.getName());
            params.put(2, ssPlayer.GetIdentifier().toString());
            params.put(3, key);
            if(SignShopConfig.SqlDbTypeSelector.equals("SQLite")){
                ((SSDatabaseSqlite)metadb).runSqliteStatement(
                        "DELETE FROM PlayerMeta WHERE Plugin = ? AND Playername = ? AND Metakey LIKE ?", params, false);
            } else {
                ((SSDatabaseH2)metadb).runH2Statement(
                        "DELETE FROM PlayerMeta WHERE Plugin = ? AND Playername = ? AND Metakey LIKE ?", params, false);
            }
        } finally {
            if(SignShopConfig.SqlDbTypeSelector.equals("SQLite")) {
                ((SSDatabaseSqlite)metadb).close();
            } else {
                ((SSDatabaseH2)metadb).close();
            }
        }
    }

    private static class ToConvert {
        public String playerName;
        public String newId;
        public String metakey;
        public String metavalue;

        private ToConvert(String pPlayername, String pNewId, String pMetakey, String pMetavalue) {
            playerName = pPlayername;
            newId = pNewId;
            metakey = pMetakey;
            metavalue = pMetavalue;
        }
    }
}
