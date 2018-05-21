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
package io.lumeer.storage.hbase.dao.project;

import io.lumeer.api.model.Collection;
import io.lumeer.api.model.Project;
import io.lumeer.api.model.ResourceType;
import io.lumeer.storage.api.dao.CollectionDao;
import io.lumeer.storage.api.exception.ResourceNotFoundException;
import io.lumeer.storage.api.query.SearchQuery;
import io.lumeer.storage.api.query.SuggestionQuery;
import io.lumeer.storage.hbase.model.HbaseCollection;
import org.apache.hadoop.hbase.TableName;

import javax.enterprise.context.RequestScoped;
import java.io.IOException;
import java.util.List;
import java.util.Set;

@RequestScoped
public class HbaseCollectionDao extends ProjectScopedDao implements CollectionDao {

   private static final String PREFIX = "collections_p-";

   @Override
   public void createCollectionsRepository(Project project) {
      try {
         datastore.createTable(databaseTable(project).toString());
      } catch (IOException e) {
         e.printStackTrace();
      }
   }

   @Override
   public void deleteCollectionsRepository(Project project) {
      try {
         datastore.deleteTable(databaseTable(project));
      } catch (IOException e) {
         e.printStackTrace();
      }
   }

   @Override
   public Collection createCollection(final Collection collection) {
      HbaseCollection hbaseCollection = new HbaseCollection(collection);
      try {
         datastore.createDocument(databaseTable(), hbaseCollection.toDataDocument());
      } catch (IOException e) {
         e.printStackTrace();
      }
      return hbaseCollection;
   }

   @Override
   public Collection updateCollection(final String id, final Collection collection) {
      HbaseCollection hbaseCollection = new HbaseCollection(collection);
      hbaseCollection.setId(id);
      try {
         datastore.updateDocument(databaseTable(), hbaseCollection.toDataDocument());
      } catch (IOException e) {
         e.printStackTrace();
      }
      return hbaseCollection;
   }

   @Override
   public void deleteCollection(final String id) {
      try {
         datastore.deleteDocument(databaseTable(), id);
      } catch (IOException e) {
         e.printStackTrace();
      }
   }

   @Override
   public Collection getCollectionByCode(final String code) {
      Collection collection = null;
      try {
         collection = new HbaseCollection(datastore.readDocument(databaseTable(), HbaseCollection.CODE, code));
      } catch (IOException e) {
         e.printStackTrace();
      }
      if (collection == null) {
         throw new ResourceNotFoundException(ResourceType.COLLECTION);
      }
      return collection;
   }

   @Override
   public List<Collection> getCollections(final SearchQuery query) {
      return null;
   }

   @Override
   public List<Collection> getCollections(final SuggestionQuery query) {
      return null;
   }

   @Override
   public List<Collection> getCollectionsByAttributes(final SuggestionQuery query) {
      return null;
   }

   @Override
   public Set<String> getAllCollectionCodes() {
      return null;
   }

   private TableName databaseTable(Project project) {
      return TableName.valueOf(PREFIX + project.getId());
   }

   TableName databaseTable() {
      if (!getProject().isPresent()) {
         throw new ResourceNotFoundException(ResourceType.PROJECT);
      }
      return databaseTable(getProject().get());
   }

}
