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
package io.lumeer.storage.hbase.dao.collection;

import io.lumeer.engine.api.data.DataDocument;
import io.lumeer.storage.api.dao.DataDao;
import io.lumeer.storage.api.query.SearchQuery;
import org.apache.hadoop.hbase.TableName;

import javax.enterprise.context.RequestScoped;
import java.io.IOException;
import java.util.List;

@RequestScoped
public class HbaseDataDao extends CollectionScopedDao implements DataDao {

   private static final String ID = "_id";
   private static final String PREFIX = "data_c-";

   @Override
   public void createDataRepository(final String collectionId) {
      try {
         datastore.createTable(dataTableName(collectionId).toString());
      } catch (IOException e) {
         e.printStackTrace();
      }
   }

   @Override
   public void deleteDataRepository(final String collectionId) {
      try {
         datastore.deleteTable(dataTableName(collectionId));
      } catch (IOException e) {
         e.printStackTrace();
      }
   }

   @Override
   public DataDocument createData(final String collectionId, final String documentId, final DataDocument data) {
      try {
         datastore.createDocument(dataTableName(collectionId), data);
      } catch (IOException e) {
         e.printStackTrace();
      }
      return data;
   }

   @Override
   public List<DataDocument> createData(final String collectionId, final List<DataDocument> data) {
      data.forEach(document -> {
         try {
            datastore.createDocument(dataTableName(collectionId), document);
         } catch (IOException e) {
            e.printStackTrace();
         }
      });
      return data;
   }

   @Override
   public DataDocument updateData(final String collectionId, final String documentId, final DataDocument data) {
      try {
         datastore.updateDocument(dataTableName(collectionId), data);
      } catch (IOException e) {
         e.printStackTrace();
      }
      return data;
   }

   @Override
   public DataDocument patchData(final String collectionId, final String documentId, final DataDocument data) {
      return null;
   }

   @Override
   public void deleteData(final String collectionId, final String documentId) {
      try {
         datastore.deleteDocument(dataTableName(collectionId), documentId);
      } catch (IOException e) {
         e.printStackTrace();
      }   }

   @Override
   public DataDocument getData(final String collectionId, final String documentId) {
      try {
         return datastore.readDocument(dataTableName(collectionId), documentId);
      } catch (IOException e) {
         e.printStackTrace();
      }
      return null;
   }

   @Override
   public List<DataDocument> getData(final String collectionId, final SearchQuery query) {
      return null;
   }

   @Override
   public long getDataCount(final String collectionId, final SearchQuery query) {
      return 0;
   }


   TableName dataTableName(String collectionId) {
      return TableName.valueOf(PREFIX + collectionId);
   }
   
}
