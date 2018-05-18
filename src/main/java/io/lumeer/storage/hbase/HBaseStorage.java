package io.lumeer.storage.hbase;

import com.google.protobuf.ServiceException;
import io.lumeer.engine.api.cache.Cache;
import io.lumeer.engine.api.cache.CacheProvider;
import io.lumeer.engine.api.data.*;
import io.lumeer.engine.api.data.Query;
import io.lumeer.engine.api.exception.DocumentNotFoundException;
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

    // Implementation

    //CHECK
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

    //CHECK
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

    //CHECK
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
        return scanner.next() != null;
    }


    public String createDocument(TableName tableName, DataDocument document) throws IOException {
        if (document.getId() == null) document.setId(HBaseUtilsOld.generateUniqueId(""));
        Table table = connection.getTable(tableName);
        Put put = new Put(Bytes.toBytes(document.getId()));
        for (Map.Entry<String, Object> entry : document.entrySet()) {
            put.addColumn(Bytes.toBytes(HBaseConstants.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(entry.getKey()), HbaseUtils.toBytes(entry.getValue()));
        }
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
            Put put = new Put(Bytes.toBytes(document.getId()));
            for (Map.Entry<String, Object> entry : document.entrySet()) {
                put.addColumn(Bytes.toBytes(HBaseConstants.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(entry.getKey()), HbaseUtils.toBytes(entry.getValue()));
            }
            table.put(put);
            ids.add(createDocument(tableName, document));
        }
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

    
    public DataDocument readDocument(TableName tableName, DataFilter filter) throws IOException {
        FilterList filterList = new FilterList();
        filterList.addFilter((Filter) filter.get());
        Scan scan = new Scan();
        scan.setFilter(filterList);
        Table table = connection.getTable(tableName);
        ResultScanner scanner = table.getScanner(scan);
        Result res = scanner.next();
        if (res == null) throw new DocumentNotFoundException(tableName + " " + filter);
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

    
    public void updateDocument(String collectionName, DataDocument updatedDocument, DataFilter filter) {

    }

    
    public void replaceDocument(String collectionName, DataDocument replaceDocument, DataFilter filter) {

    }

    public void dropDocument(String collectionName, DataFilter filter) {

    }

    public void dropManyDocuments(String collectionName, DataFilter filter) {

    }

    
    public void renameAttribute(String collectionName, String oldName, String newName) {

    }

    
    public void dropAttribute(String collectionName, DataFilter filter, String attributeName) {

    }

    
    public <T> void addItemToArray(String collectionName, DataFilter filter, String attributeName, T item) {

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
        if (limit > 0) scan.setLimit(0);
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
