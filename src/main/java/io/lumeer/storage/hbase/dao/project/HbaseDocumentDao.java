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

import io.lumeer.api.model.Document;
import io.lumeer.api.model.Project;
import io.lumeer.api.model.ResourceType;
import io.lumeer.storage.api.dao.DocumentDao;
import io.lumeer.storage.api.exception.ResourceNotFoundException;
import io.lumeer.storage.hbase.model.HbaseDocument;
import org.apache.hadoop.hbase.TableName;

import javax.enterprise.context.RequestScoped;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

@RequestScoped
public class HbaseDocumentDao extends ProjectScopedDao implements DocumentDao {

   private static final String PREFIX = "documents_p-";

   @Override
   public void createDocumentsRepository(final Project project) {
      try {
         datastore.createTable(databaseTable(project).toString());
      } catch (IOException e) {
         e.printStackTrace();
      }
   }

   @Override
   public void deleteDocumentsRepository(final Project project) {
      try {
         datastore.deleteTable(databaseTable(project));
      } catch (IOException e) {
         e.printStackTrace();
      }
   }

   @Override
   public Document createDocument(final Document document) {
      HbaseDocument hbaseDocument = new HbaseDocument(document);
      try {
         datastore.createDocument(databaseTable(), hbaseDocument.toDataDocument());
      } catch (IOException e) {
         e.printStackTrace();
      }
      return hbaseDocument;
   }

   @Override
   public List<Document> createDocuments(final List<Document> documents) {
      List<HbaseDocument> hbaseDocuments = new LinkedList<>();
      documents.forEach(document -> hbaseDocuments.add(new HbaseDocument(document)));
      hbaseDocuments.forEach(hbdoc -> {
         try {
            datastore.createDocument(databaseTable(), hbdoc.toDataDocument());
         } catch (IOException e) {
            e.printStackTrace();
         }
      });

      return documents;
   }

   @Override
   public Document updateDocument(final String id, final Document document) {
      HbaseDocument hbaseDocument = new HbaseDocument(document);
      hbaseDocument.setId(id);
      try {
         datastore.updateDocument(databaseTable(), hbaseDocument.toDataDocument());
      } catch (IOException e) {
         e.printStackTrace();
      }
      return hbaseDocument;
   }

   @Override
   public void deleteDocument(final String id) {
      try {
         datastore.deleteDocument(databaseTable(), id);
      } catch (IOException e) {
         e.printStackTrace();
      }
   }

   @Override
   public void deleteDocuments(final String collectionId) {
   }

   @Override
   public Document getDocumentById(final String id) {
      Document document = null;
      try {
         document = new HbaseDocument(datastore.readDocument(databaseTable(), id));
      } catch (IOException e) {
         e.printStackTrace();
      }
      if (document == null) {
         throw new ResourceNotFoundException(ResourceType.DOCUMENT);
      }
      return document;
   }

   @Override
   public List<Document> getDocumentsByIds(final String... ids) {
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
