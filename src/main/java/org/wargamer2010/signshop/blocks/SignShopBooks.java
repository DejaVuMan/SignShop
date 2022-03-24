package org.wargamer2010.signshop.blocks;

import org.bukkit.inventory.ItemStack;
import org.wargamer2010.signshop.SignShop;
import org.wargamer2010.signshop.configuration.SignShopConfig;
import org.wargamer2010.signshop.util.itemUtil;
import org.wargamer2010.signshop.util.signshopUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Level;

public class SignShopBooks {
    private static final char pageSeperator = (char)3;
    private static final String filename = "books.db";

    private SignShopBooks() {

    }

    public static void init() {
        if(SignShopConfig.getDbType().equals("SQLite")){
            SSDatabaseSqlite db = new SSDatabaseSqlite(filename);

            if(!db.tableExists("Book")) {
                db.runSqliteStatement("CREATE TABLE Book ( BookID INTEGER, Title TEXT NOT NULL, Author VARCHAR(200) NOT NULL, Pages TEXT, "
                        + "Generation INTEGER NOT NULL DEFAULT -1, PRIMARY KEY(BookID) )", null, false);
                db.close();
            } else if(!db.columnExists("Generation")) {
                db.open();
                db.runSqliteStatement("ALTER TABLE Book ADD COLUMN Generation INTEGER NOT NULL DEFAULT -1;", null, false);
                db.close();
            }
        } else {
            SSDatabaseH2 db = new SSDatabaseH2(filename);
            // H2 differs, as we can't make use of the last_insert_rowid() function since its SQLite-only
            // instead, we add an aditional column to the table, RowID, which will auto-increment by 1
            if(!db.tableExists("Book")) {
                db.runH2Statement("CREATE TABLE Book ( BookID INTEGER, Title TEXT NOT NULL, Author VARCHAR(200) NOT NULL, Pages TEXT, "
                        + "Generation INTEGER NOT NULL DEFAULT -1, RowID INTEGER NOT NULL AUTO_INCREMENT, PRIMARY KEY(BookID) )", null, false);
                db.close();
            } else if(!db.columnExists("Generation")) {
                db.open();
                db.runH2Statement("ALTER TABLE Book ADD COLUMN Generation INTEGER NOT NULL DEFAULT -1;", null, false);
                db.close();
            }
        }
    }

    public static <T> void addBook(ItemStack bookStack) {
        Integer tempID = getBookID(bookStack);
        if(tempID > -1)
            return;

        IBookItem item = BookFactory.getBookItem(bookStack);
        Map<Integer, Object> pars = new LinkedHashMap<>();
        pars.put(1, (item.getTitle() == null) ? "" : item.getTitle());
        pars.put(2, (item.getAuthor() == null) ? "" : item.getAuthor());
        pars.put(3, signshopUtil.implode(item.getPages(), String.valueOf(pageSeperator)));
        Integer gen = item.getGeneration();
        pars.put(4, gen == null ? -1 : gen);

        T db;

        if(SignShopConfig.getDbType().equals("SQLite")){
            db = (T) new SSDatabaseSqlite(filename);
        } else {
            db = (T) new SSDatabaseH2(filename);
        }

        if(SignShopConfig.getDbType().equals("SQLite")){
            try {
                Integer ID = (Integer) ((SSDatabaseSqlite) db).runSqliteStatement(
                        "INSERT INTO Book(Title, Author, Pages, Generation) VALUES (?, ?, ?, ?);", pars, false);
            } finally {
                ((SSDatabaseSqlite)db).close();
            }
        } else {
            try {
                Integer ID = (Integer) ((SSDatabaseH2)db).runH2Statement(
                        "INSERT INTO Book(Title, Author, Pages, Generation) VALUES (?, ?, ?, ?);", pars, false);
            } finally {
                ((SSDatabaseH2)db).close();
            }
        }
    }

    public static <T> void removeBook(Integer id) {
        Map<Integer, Object> pars = new LinkedHashMap<>();
        pars.put(1, id);
        T db;

        if(SignShopConfig.getDbType().equals("SQLite")){
            db = (T) new SSDatabaseSqlite(filename); // Unchecked Cast will do for now
        } else {
            db = (T) new SSDatabaseH2(filename);
        }

        if(SignShopConfig.getDbType().equals("SQLite")){
            try {
                ((SSDatabaseSqlite)db).runSqliteStatement("DELETE FROM Book WHERE BookID = ?;", pars, false);
            } finally {
                ((SSDatabaseSqlite)db).close();
            }
        } else {
            try {
                ((SSDatabaseH2)db).runH2Statement("DELETE FROM Book WHERE BookID = ?;", pars, false);
            } finally {
                ((SSDatabaseH2)db).close();
            }
        }


    }

    public static <T> Integer getBookID(ItemStack bookStack) {
        if(!itemUtil.isWriteableBook(bookStack))
            return -1;
        IBookItem item = BookFactory.getBookItem(bookStack);
        Map<Integer, Object> pars = new LinkedHashMap<>();
        pars.put(1, (item.getTitle() == null) ? "" : item.getTitle());
        pars.put(2, (item.getAuthor() == null) ? "" : item.getAuthor());
        pars.put(3, signshopUtil.implode(item.getPages(), String.valueOf(pageSeperator)));
        Integer gen = item.getGeneration();
        pars.put(4, gen == null ? -1 : gen);
        Integer ID = null;
        T db; // temporary generic which we set to SSDatabaseSqlite or SSDatabaseH2

        if(SignShopConfig.getDbType().equals("SQLite")){
            db = (T) new SSDatabaseSqlite(filename);
        } else {
            db = (T) new SSDatabaseH2(filename);
        }
        try {
            ResultSet set;
            if(SignShopConfig.getDbType().equals("SQLite")){
                set = (ResultSet)((SSDatabaseSqlite)db).runSqliteStatement("SELECT BookID FROM Book WHERE Title = ? AND Author = ? AND Pages = ? AND Generation = ?;", pars, true);
            } else {
                set = (ResultSet)((SSDatabaseH2)db).runH2Statement("SELECT BookID FROM Book WHERE Title = ? AND Author = ? AND Pages = ? AND Generation = ?;", pars, true);
            }

            if(set != null && set.next())
                ID = set.getInt("BookID");
            else
                ID = -1;
        } catch (SQLException ex) {
            SignShop.log("BookID was not found in result from SELECT query.", Level.WARNING);
        } finally {
            if(SignShopConfig.getDbType().equals("SQLite")){
                ((SSDatabaseSqlite)db).close();
            } else {
                ((SSDatabaseH2)db).close();
            }
        }

        if(ID == null)
            return -1;
        else
            return ID;
    }

    public static <T> ItemStack addBooksProps(ItemStack bookStack, Integer id) {
        Map<Integer, Object> pars = new LinkedHashMap<>();
        pars.put(1, id);
        ResultSet set;
        T db;
        if(SignShopConfig.getDbType().equals("SQLite")){
            db = (T) new SSDatabaseSqlite(filename); // can we declare a generic var and narrow it later?
            set = (ResultSet) ((SSDatabaseSqlite) db).runSqliteStatement("SELECT * FROM Book WHERE BookID = ?", pars, true);
        } else {
            db = (T) new SSDatabaseH2(filename);
            set = (ResultSet) ((SSDatabaseH2) db).runH2Statement("SELECT * FROM Book WHERE BookID = ?", pars, true);
        }
        if(set == null)
            return bookStack;
        IBookItem item = null;
        try {
            item = BookFactory.getBookItem(bookStack);
            item.setAuthor(set.getString("Author"));
            item.setTitle(set.getString("Title"));
            item.setPages(set.getString("Pages").split(String.valueOf(pageSeperator)));
            int gen = set.getInt("Generation");
            item.setGeneration(gen == -1 ? null : set.getInt("Generation"));
        } catch (SQLException ignored) {

        } finally {
            if(SignShopConfig.getDbType().equals("SQLite")){
                ((SSDatabaseSqlite)db).close();
            } else {
                ((SSDatabaseH2)db).close();
            }
        }
        return (item == null ? bookStack : item.getStack());
    }
}
