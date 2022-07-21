package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 *
 * @Threadsafe
 */
public class Catalog {

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    private  class Table{//新增Table类
        int FileId;
        DbFile File;
        String FileName;
        String Filepkey;
        public Table(int i, DbFile f, String n, String pk){
            FileId = i;
            File = f;
            FileName = n;
            Filepkey = pk;
        }

    }
    List<Table> tables = new ArrayList<>();

    public Catalog() {
        // some code goes here
        tables = new ArrayList<>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {//向CataLog中添加表。主键可为空、name可为空，此时随机一个UUID作为其name。
        // some code goes here
        Table table = new Table(file.getId(), file, name, pkeyField);
        for(Table origintable : tables) {
            if (origintable.FileId == file.getId()) {
                origintable.File = file;
                origintable.FileName = name;
                origintable.Filepkey = pkeyField;
                return;
            }
            if (origintable.FileName.equals(name)) {
                origintable.File = file;
                origintable.Filepkey = pkeyField;
                origintable.FileId = file.getId();
                return;
            }
        }
        this.tables.add(table);
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {//通过表名获得表id
        // some code goes here
        for(Table table : this.tables){
            if(name == table.FileName){
                return table.FileId;
            }
        }
        throw new NoSuchElementException(String.format("名为%s的表不存在",name));
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {//通过表id 获得表的TupleDesc
        // some code goes here
        for(Table table : this.tables){
            if(tableid == table.FileId){
                return table.File.getTupleDesc();
            }
        }
        throw new NoSuchElementException(String.format("ID为%d的表不存在",tableid));
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {//通过表id 获得表的内容DbFile
        // some code goes here
        for(Table table : tables){
            if(table.FileId == tableid){
                return table.File;
            }
        }
        throw new NoSuchElementException(String.format("ID为%d的表不存在",tableid));
    }

    public String getPrimaryKey(int tableid) {//通过表id 获得表的主键
        // some code goes here
        for(Table table : tables){
            if(table.FileId == tableid){
                return table.Filepkey;
            }
        }
        throw new NoSuchElementException(String.format("ID为%d的表不存在",tableid));
    }

    public Iterator<Integer> tableIdIterator() {//返回tableId的迭代器
        //不知道这样实现可不可以?to be finished
      //   some code goes here
        List<Integer> tdTableIdList = new ArrayList<>();
        for(Table table : this.tables){
            tdTableIdList.add(table.FileId);
        }
        return tdTableIdList.iterator();
    }

    public String getTableName(int id) {//通过表id 获得表名
        // some code goes here
        for(Table table : tables){
            if(table.FileId == id) return table.FileName;
        }
        throw new NoSuchElementException(String.format("ID为%d的表不存在",id));
    }

    /** Delete all tables from the catalog */
    public void clear() {//清空Catalog
        // some code goes here
        this.tables.clear();
    }

    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));

            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

