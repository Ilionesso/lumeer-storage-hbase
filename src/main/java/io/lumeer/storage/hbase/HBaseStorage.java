package io.lumeer.storage.hbase;

import com.google.protobuf.ServiceException;
import io.lumeer.engine.api.LumeerConst;
import io.lumeer.engine.api.cache.Cache;
import io.lumeer.engine.api.cache.CacheProvider;
import io.lumeer.engine.api.data.*;
import io.lumeer.engine.api.data.Query;
import io.lumeer.engine.api.exception.UnsuccessfulOperationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

/**
 * Created by Ilia Sheiko on 12/04/2018.
 */
public class HBaseStorage{

    private static final String COLLECTION_CACHE = "collections";
    private static final String DEFAULT_FAMILY = "DEFAULT_FAMILY";
    private static final String DEFAULT_SNAPSHOT_NAME = "defaultSnapshot";

    private Configuration config;
    private Admin admin;
    private Connection connection;
    private AggregationClient aggregationClient;



    //Cache things

    private Cache<List<String>> collectionsCache;

    private List<String> getCollectionCache() {
        return collectionsCache.get();
    }

    private void setCollectionCache(final List<String> collections) {
        collectionsCache.set(collections);
    }

    public void setCacheProvider(CacheProvider cacheProvider) {
        this.collectionsCache = cacheProvider.getCache(COLLECTION_CACHE);
    }

    //CONNECTING

    public void connect() throws IOException, ServiceException {
        config = HBaseConfiguration.create();

        String path = this.getClass()
                .getClassLoader()
                .getResource("hbase-site.xml")
                .getPath();
        config.addResource(new Path(path));
        connection = ConnectionFactory.createConnection(config);
        admin = connection.getAdmin();
        aggregationClient = new AggregationClient(config);
        HBaseAdmin.checkHBaseAvailable(config);
    }

    public void disconnect() throws IOException {
        if (admin != null) {
            admin.shutdown();
            admin.close();
        }
    }

    //TABLES LISTING
    public List<HTableDescriptor> getAllTables() throws IOException {
        return Arrays.stream(admin.listTables()).
                collect(Collectors.toCollection(ArrayList::new));
    }

    public List<TableName> getAllTableNames() throws IOException {
        return Arrays.stream(admin.listTables()).
                map(HTableDescriptor::getTableName).
                collect(Collectors.toCollection(ArrayList::new));
    }

    public List<String> getAllTableNamesStrings() throws IOException {
        return Arrays.stream(admin.listTables()).
                map(HTableDescriptor::getNameAsString).
                collect(Collectors.toCollection(ArrayList::new));
    }

    public int getAllTablesCount() throws IOException {
        return Math.toIntExact(Arrays.stream(admin.listTables()).count());
    }

    public boolean hasTable(TableName tableName) throws IOException {
        return admin.tableExists(tableName);
    }

    public Table getTable(TableName tableName) throws IOException {
        return connection.getTable(tableName);
    }

    public void createTable(final String tableName) throws IOException {
        HTableDescriptor desc = new HTableDescriptor(toBytes(tableName));

        desc.addFamily(new HColumnDescriptor(toBytes(HBaseConstants.DEFAULT_COLUMN_FAMILY)));
        if (collectionsCache != null) {
            collectionsCache.lock(COLLECTION_CACHE);
            try {
                final List<String> collections = getCollectionCache();

                if (collections != null) {
                    collections.add(tableName);
                } else {
                    setCollectionCache(new ArrayList<>(Collections.singletonList(tableName)));
                }
                admin.createTable(desc);
            } finally {
                collectionsCache.unlock(COLLECTION_CACHE);
            }
        } else {
            admin.createTable(desc);
        }
    }


    private void disableTable(TableName tableName) throws IOException {
        if (!hasTable(tableName)) return;
        if (admin.isTableEnabled(tableName))
            admin.disableTable(tableName);
    }


    private void enableTable(TableName tableName) throws IOException {
        if (!hasTable(tableName)) return;
        if (admin.isTableDisabled(tableName))
            admin.enableTable(tableName);
    }


    public void deleteTable(TableName tableName) throws IOException {
        if (!hasTable(tableName)) return;
        if (collectionsCache != null) {
            collectionsCache.lock(COLLECTION_CACHE);
            try {
                final List<String> collections = getCollectionCache();

                if (collections != null) {
                    collections.remove(tableName.getNameAsString());
                }

                disableTable(tableName);
                admin.deleteTable(tableName);
            } finally {
                collectionsCache.unlock(COLLECTION_CACHE);
            }
        } else {
            disableTable(tableName);
            admin.deleteTable(tableName);
        }
    }


    public void renameTable(TableName oldTableName, TableName newTableName) throws IOException {
        if (!hasTable(oldTableName)) return;
        disableTable(oldTableName);
        admin.snapshot(DEFAULT_SNAPSHOT_NAME, oldTableName);
        admin.cloneSnapshot(DEFAULT_SNAPSHOT_NAME, newTableName);
        admin.deleteSnapshot(DEFAULT_SNAPSHOT_NAME);
        deleteTable(oldTableName);
        enableTable(newTableName);
    }

    
    public long documentCount(String collectionName) throws Throwable {
        return 0;
    }

    
    public boolean tableHasDocument(TableName tableName, DataFilter filter) throws IOException {
        Filter firstOnlyFilter = new FirstKeyOnlyFilter();
        FilterList filterList = new FilterList();
        filterList.addFilter(firstOnlyFilter);
        filterList.addFilter((Filter) filter.get());
        Scan scan = new Scan();
        scan.setFilter(filterList);
        Table table = connection.getTable(tableName);
        ResultScanner scanner = table.getScanner(scan);
        table.close();
        return scanner.next() != null;
    }

    private Put makePutFromDataDocument(DataDocument document){
        Put put = new Put(Bytes.toBytes(document.getId()));
        return addValuesToPut(put, document);
    }

    private Put makePutFromDataDocument(DataDocument document, String id){
        Put put = new Put(Bytes.toBytes(id));
        return addValuesToPut(put, document);
    }

    private Put addValuesToPut(Put put, DataDocument document){
        for (Map.Entry<String, Object> entry : document.entrySet())
            put.addColumn(Bytes.toBytes(HBaseConstants.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(entry.getKey()), HbaseUtils.toBytes(entry.getValue()));
        return put;
    }


    public String createDocument(TableName tableName, DataDocument document) throws IOException {
        if (document.getId() == null) document.setId(HBaseUtilsOld.generateUniqueId(""));
        Table table = connection.getTable(tableName);
        Put put = makePutFromDataDocument(document);
        table.put(put);
        table.close();
        return document.getId();
    }


//    /**
//     * Lazy/DRY
//     * @param tableName
//     * @param dataDocuments
//     * @return
//     * @throws IOException
//     */
//    public List<String> createDocuments(TableName tableName, List<DataDocument> dataDocuments) throws IOException {
//        List<String> ids = new LinkedList<>();
//        for (DataDocument doc :dataDocuments){
//            ids.add(createDocument(tableName, doc));
//        }
//        return null;
//    }

    /**
     * Not lazy/DRY?
     * @param tableName
     * @param dataDocuments
     * @return
     * @throws IOException
     */
    public List<String> createDocuments(TableName tableName, List<DataDocument> dataDocuments) throws IOException {
        List<String> ids = new LinkedList<>();
        Table table = connection.getTable(tableName);
        for (DataDocument document :dataDocuments){
            if (document.getId() == null) document.setId(HBaseUtilsOld.generateUniqueId(""));
            Put put = makePutFromDataDocument(document);
            table.put(put);
            ids.add(createDocument(tableName, document));
        }
        table.close();
        return ids;
    }

    //Versions?
    public void createOldDocument(String collectionName, DataDocument document, String documentId, int version) throws UnsuccessfulOperationException {

    }

    
    public DataDocument readDocumentIncludeAttrs(String collectionName, DataFilter filter, List<String> attributes) {
        return null;
    }

    private String getQualifierFromCell(Cell cell){
        return Bytes.toString(CellUtil.cloneQualifier(cell));
    }

    private Object getValueFromCell(Cell cell){
        return HbaseUtils.deserialize(CellUtil.cloneValue(cell));
    }

    private ResultScanner scan(TableName tableName, DataFilter filter) throws IOException {
        FilterList filterList = new FilterList();
        if (filter != null)
            filterList.addFilter((Filter) filter.get());
        Scan scan = new Scan();
        scan.setFilter(filterList);
        Table table = connection.getTable(tableName);
        return table.getScanner(scan);
    }

    public HBaseDataDocument readDocument(TableName tableName, DataFilter filter) throws IOException {
        ResultScanner scanner = scan(tableName, filter);
        Result res = scanner.next();
        if (res == null) return null;
        Map<String, Object> docMap = Arrays.stream(res.rawCells()).
                collect(Collectors.toMap(this::getQualifierFromCell, this::getValueFromCell));
//        for (Result res : scanner) {
//            Arrays.stream(res.rawCells()).forEach(cell -> {
//                byte[] column = CellUtil.cloneQualifier(cell);
//                byte[] value = CellUtil.cloneValue(cell);
//                System.out.println(Bytes.toString(column));
//                System.out.println(HbaseUtils.deserialize(value).toString());
//            });
//        }
        return new HBaseDataDocument(docMap);
    }

    private void put(TableName tableName, Put put) throws IOException {
        Table table = connection.getTable(tableName);
        table.put(put);
        table.close();
    }

    //Inject scan and do not make damn documents
    public void updateDocument(TableName tableName, DataDocument update, DataFilter filter) throws IOException {
        if (update.containsKey(LumeerConst.Document.ID))
            update.remove(LumeerConst.Document.ID);
        DataDocument toUpdate = readDocument(tableName, filter);
        Put put = makePutFromDataDocument(update, toUpdate.getId());
        put(tableName, put);
    }

    public void replaceDocument(TableName tableName, DataDocument replaceDocument, DataFilter filter) throws IOException {
        if (replaceDocument.containsKey(LumeerConst.Document.ID))
            replaceDocument.remove(LumeerConst.Document.ID);
        ResultScanner results = scan(tableName, filter);
        Result toReplace = results.next();
        if (toReplace == null) return;
        deleteRow(tableName, toReplace.getRow());
        Put put = makePutFromDataDocument(replaceDocument, HbaseUtils.getResultId(toReplace));
        put(tableName, put);
    }

    public void deleteDocument(TableName tableName, DataFilter filter) throws IOException {
        ResultScanner results = scan(tableName, filter);
        Result toDelete = results.next();
        if (toDelete == null) return;
        deleteRow(tableName, toDelete.getRow());
    }

    private void deleteRow(TableName tableName, byte[] row) throws IOException {
        Delete toDelete = new Delete(row);
        Table table = connection.getTable(tableName);
        table.delete(toDelete);
        table.close();
    }

    private void deleteList(TableName tableName, List<Delete> toDelete) throws IOException {
        if (toDelete.size() == 0) return;
        Table table = connection.getTable(tableName);
        table.delete(toDelete);
        table.close();
    }

    private void putList(TableName tableName, List<Put> toPut) throws IOException {
        if (toPut.size() == 0) return;
        Table table = connection.getTable(tableName);
        table.put(toPut);
        table.close();
    }

    public void deleteManyDocuments(TableName tableName, DataFilter filter) throws IOException {
        ResultScanner results = scan(tableName, filter);
        List<Delete> toDeleteList = new ArrayList<>();
        for (Result result : results)
            toDeleteList.add(new Delete(result.getRow()));
        deleteList(tableName, toDeleteList);
    }

    
    public void renameQualifier(TableName tableName, String oldName, String newName) throws IOException {
        ResultScanner results = scan(tableName, null);
        List<Put> toPutList = new ArrayList<>();
        List<Delete> toDeleteList = new ArrayList<>();
        for (Result result : results){
            Put toPut = new Put(result.getRow());
            Delete toDelete = new Delete(result.getRow());
            byte[] value = result.getValue(Bytes.toBytes(HBaseConstants.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(oldName));
            toPut.addColumn(Bytes.toBytes(HBaseConstants.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(newName), value);
            toDelete.addColumn(Bytes.toBytes(HBaseConstants.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(oldName));
            toPutList.add(toPut);
            toDeleteList.add(toDelete);
        }
        deleteList(tableName, toDeleteList);
        putList(tableName, toPutList);
    }

    
    public void dropQualifier(TableName tableName, DataFilter filter, String qualifierName) throws IOException {
        ResultScanner results = scan(tableName, filter);
        List<Delete> toDeleteList = new ArrayList<>();
        for (Result result : results){
            Delete toDelete = new Delete(result.getRow());
            toDelete.addColumns(Bytes.toBytes(HBaseConstants.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(qualifierName));
            toDeleteList.add(toDelete);
        }
        deleteList(tableName, toDeleteList);
    }

    
    public <T> void addItemToArray(TableName tableName, DataFilter filter, String qualifierName, T item) throws IOException {
//       DataDocument doc = readDocument(tableName, filter);
//       List<T> list = new LinkedList<T>();
//       doc.getArrayList(qualifierName, T);
    }

    
    public <T> void addItemsToArray(String collectionName, DataFilter filter, String attributeName, List<T> items) {

    }

    
    public <T> void removeItemFromArray(String collectionName, DataFilter filter, String attributeName, T item) {

    }

    
    public <T> void removeItemsFromArray(String collectionName, DataFilter filter, String attributeName, List<T> items) {

    }

    
    public Set<String> getAttributeValues(String collectionName, String attributeName) {
        return null;
    }

    
    public List<DataDocument> run(String command) {
        return null;
    }

    
    public List<DataDocument> run(DataDocument command) {
        return null;
    }

    
    public List<DataDocument> search(String collectionName, DataFilter filter, List<String> attributes) {
        return null;
    }

    
    public List<DataDocument> search(TableName tableName, DataFilter filter, DataSort sort, int skip, int limit) throws IOException {
        Scan scan = new Scan();
        if (filter != null) {
            FilterList filterList = new FilterList();
            filterList.addFilter((Filter) filter.get());
            scan.setFilter(filterList);
        }
        if (limit > 0) scan.setLimit(limit);
        //TODO SKIP
        //TODO SORT
        Table table = connection.getTable(tableName);
        ResultScanner scanner = table.getScanner(scan);
        LinkedList<DataDocument> documents = new LinkedList<>();
        for (Result res : scanner) {
            Map<String, Object> docMap = Arrays.stream(res.rawCells()).
                    collect(Collectors.toMap(this::getQualifierFromCell, this::getValueFromCell));
            documents.add(new HBaseDataDocument(docMap));
        }
        return documents;
    }

    
    public List<DataDocument> search(String collectionName, DataFilter filter, DataSort sort, List<String> attributes, int skip, int limit) {
        return null;
    }

    
    public long count(String collectionName, DataFilter filter) {
        return 0;
    }

    
    public List<DataDocument> query(Query query) {
        return null;
    }

    
    public List<DataDocument> aggregate(String collectionName, DataDocument... stages) {
        return null;
    }

    
    public void incrementAttributeValueBy(String collectionName, DataFilter filter, String attributeName, int incBy) {

    }

    
    public int getNextSequenceNo(String collectionName, String indexAttribute, String index) {
        return 0;
    }

    
    public void resetSequence(String collectionName, String indexAttribute, String index) {

    }

    
    public void createIndex(String collectionName, DataDocument indexAttributes, boolean unique) {

    }

    
    public List<DataDocument> listIndexes(String collectionName) {
        return null;
    }

    
    public void dropIndex(String collectionName, String indexName) {

    }

    
    public void invalidateCaches() {

    }

    
    public DataStorageStats getDbStats() {
        return null;
    }

    
    public DataStorageStats getCollectionStats(String collectionName) {
        return null;
    }

    
    public Object getDatabase() {
        return null;
    }

    
    public Object getDataStore() {
        return null;
    }

}
