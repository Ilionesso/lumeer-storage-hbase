/*
 * Lumeer: Modern Data Definition and Processing Platform
 *
 * Copyright (C) since 2017 Answer Institute, s.r.o. and/or its affiliates.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.lumeer.storage.hbase;

import io.lumeer.engine.api.LumeerConst;
import io.lumeer.engine.api.data.DataDocument;
import io.lumeer.engine.api.data.DataFilter;
import io.lumeer.engine.api.data.DataSort;
import io.lumeer.engine.api.data.DataStorageDialect;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import javax.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="mailto:marvenec@gmail.com">Martin Večeřa</a>
 */
@ApplicationScoped
public class HBaseDbStorageDialect implements DataStorageDialect {


    @Override
    public DataDocument renameAttributeQuery(String metadataCollection, String collection, String oldName, String newName) {
        return null;
    }

    @Override
    public DataDocument addRecentlyUsedDocumentQuery(String metadataCollection, String collection, String id, int listSize) {
        return null;
    }

    @Override
    public DataDocument[] usersOfGroupAggregate(String organization, String group) {
        return new DataDocument[0];
    }

    @Override
    public DataFilter fieldValueFilter(String fieldName, Object value) {
        SingleColumnValueFilter filter = new SingleColumnValueFilter(
                Bytes.toBytes(HBaseConstants.DEFAULT_COLUMN_FAMILY),
                Bytes.toBytes(fieldName),
                CompareFilter.CompareOp.EQUAL,
                HbaseUtils.toBytes(value)
        );
        return new HBaseDbDataFilter(filter);
    }

    @Override
    public DataFilter fieldValueWildcardFilter(String fieldName, Object valuePart) {
        return null;
    }

    @Override
    public DataFilter fieldValueWildcardFilterOneSided(String fieldName, Object valuePart) {
        return null;
    }

    @Override
    public DataFilter documentFilter(String documentFilter) {
        return null;
    }

    @Override
    public DataFilter documentNestedIdFilter(String documentId) {
        return null;
    }

    @Override
    public DataFilter documentNestedIdFilterWithVersion(String documentId, int version) {
        return null;
    }

    @Override
    public DataFilter documentIdFilter(String documentId) {
        return fieldValueFilter(LumeerConst.Document.ID, documentId);
    }

    @Override
    public DataFilter multipleFieldsValueFilter(Map<String, Object> fields) {
        return null;
    }

    @Override
    public DataFilter combineFilters(DataFilter... filters) {
        return null;
    }

    @Override
    public DataFilter collectionPermissionsRoleFilter(String role, String user, List<String> groups) {
        return null;
    }

    @Override
    public DataSort documentSort(String documentSort) {
        return null;
    }

    @Override
    public DataSort documentFieldSort(String fieldName, int sortOrder) {
        return null;
    }

    @Override
    public String concatFields(String... fields) {
        return null;
    }

    private DataFilter createFilter(final Filter filter) {
        return new HBaseDbDataFilter(filter);
    }
}
