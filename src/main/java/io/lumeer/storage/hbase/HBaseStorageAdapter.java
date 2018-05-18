package io.lumeer.storage.hbase;

import com.google.protobuf.ServiceException;
import io.lumeer.engine.api.cache.CacheProvider;
import io.lumeer.engine.api.data.*;
import io.lumeer.engine.api.exception.UnsuccessfulOperationException;
import org.apache.hadoop.hbase.TableName;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Created by Ilia Sheiko on 17/04/2018.
 */
public class HBaseStorageAdapter implements DataStorage{


    private HBaseStorage hBaseStorage;

    public HBaseStorageAdapter() {
        hBaseStorage = new HBaseStorage();
    }

    @Override
    public void setCacheProvider(CacheProvider cacheProvider) {

    }

    @Override
    public void connect(List<StorageConnection> connections, String database, Boolean useSsl) {
        try {
            hBaseStorage.connect();
        } catch (IOException | ServiceException e) {
            e.printStackTrace();
        }
    }

    public void connect() {
        try {
            hBaseStorage.connect();
        } catch (IOException | ServiceException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void connect(StorageConnection connection, String database, Boolean useSsl) {
        try {
            hBaseStorage.connect();
        } catch (IOException | ServiceException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void disconnect() {

    }

    @Override
    public List<String> getAllCollections() {
        try {
            return hBaseStorage.getAllTableNamesStrings();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void createCollection(String collectionName) {
        try {
            hBaseStorage.createTable(collectionName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void dropCollection(String collectionName) {
        try {
            hBaseStorage.deleteTable(TableName.valueOf(collectionName));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void renameCollection(String oldCollectionName, String newCollectionName) {
        try {
            hBaseStorage.renameTable(TableName.valueOf(oldCollectionName), TableName.valueOf(newCollectionName));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean hasCollection(String collectionName) {
        try {
            return hBaseStorage.hasTable(TableName.valueOf(collectionName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public long documentCount(String collectionName) {
        return 0;
    }

    @Override
    public boolean collectionHasDocument(String collectionName, DataFilter filter) {
        try {
            return hBaseStorage.tableHasDocument(TableName.valueOf(collectionName), filter);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public String createDocument(String collectionName, DataDocument document) {
        try {
            return hBaseStorage.createDocument(TableName.valueOf(collectionName), document);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public List<String> createDocuments(String collectionName, List<DataDocument> dataDocuments) {
        try {
            return hBaseStorage.createDocuments(TableName.valueOf(collectionName), dataDocuments);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void createOldDocument(String collectionName, DataDocument document, String documentId, int version) throws UnsuccessfulOperationException {

    }

    @Override
    public DataDocument readDocumentIncludeAttrs(String collectionName, DataFilter filter, List<String> attributes) {
        return null;
    }

    @Override
    public DataDocument readDocument(String collectionName, DataFilter filter) {
        try {
            return hBaseStorage.readDocument(TableName.valueOf(collectionName), filter);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void updateDocument(String collectionName, DataDocument updatedDocument, DataFilter filter) {

    }

    @Override
    public void replaceDocument(String collectionName, DataDocument replaceDocument, DataFilter filter) {

    }

    @Override
    public void dropDocument(String collectionName, DataFilter filter) {

    }

    @Override
    public void dropManyDocuments(String collectionName, DataFilter filter) {

    }

    @Override
    public void renameAttribute(String collectionName, String oldName, String newName) {

    }

    @Override
    public void dropAttribute(String collectionName, DataFilter filter, String attributeName) {

    }

    @Override
    public <T> void addItemToArray(String collectionName, DataFilter filter, String attributeName, T item) {

    }

    @Override
    public <T> void addItemsToArray(String collectionName, DataFilter filter, String attributeName, List<T> items) {

    }

    @Override
    public <T> void removeItemFromArray(String collectionName, DataFilter filter, String attributeName, T item) {

    }

    @Override
    public <T> void removeItemsFromArray(String collectionName, DataFilter filter, String attributeName, List<T> items) {

    }

    @Override
    public Set<String> getAttributeValues(String collectionName, String attributeName) {
        return null;
    }

    @Override
    public List<DataDocument> run(String command) {
        return null;
    }

    @Override
    public List<DataDocument> run(DataDocument command) {
        return null;
    }

    @Override
    public List<DataDocument> search(String collectionName, DataFilter filter, List<String> attributes) {
        return null;
    }

    @Override
    public List<DataDocument> search(String collectionName, DataFilter filter, DataSort sort, int skip, int limit) {
        try {
            return hBaseStorage.search(TableName.valueOf(collectionName), filter, sort, skip, limit);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public List<DataDocument> search(String collectionName, DataFilter filter, DataSort sort, List<String> attributes, int skip, int limit) {
        return null;
    }

    @Override
    public long count(String collectionName, DataFilter filter) {
        return 0;
    }

    @Override
    public List<DataDocument> query(Query query) {
        return null;
    }

    @Override
    public List<DataDocument> aggregate(String collectionName, DataDocument... stages) {
        return null;
    }

    @Override
    public void incrementAttributeValueBy(String collectionName, DataFilter filter, String attributeName, int incBy) {

    }

    @Override
    public int getNextSequenceNo(String collectionName, String indexAttribute, String index) {
        return 0;
    }

    @Override
    public void resetSequence(String collectionName, String indexAttribute, String index) {

    }

    @Override
    public void createIndex(String collectionName, DataDocument indexAttributes, boolean unique) {

    }

    @Override
    public List<DataDocument> listIndexes(String collectionName) {
        return null;
    }

    @Override
    public void dropIndex(String collectionName, String indexName) {

    }

    @Override
    public void invalidateCaches() {

    }

    @Override
    public DataStorageStats getDbStats() {
        return null;
    }

    @Override
    public DataStorageStats getCollectionStats(String collectionName) {
        return null;
    }

    @Override
    public Object getDatabase() {
        return null;
    }

    @Override
    public Object getDataStore() {
        return null;
    }
}
