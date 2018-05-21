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
package io.lumeer.storage.hbase.dao.system;

import io.lumeer.api.model.Organization;
import io.lumeer.api.model.ResourceType;
import io.lumeer.engine.api.LumeerConst;
import io.lumeer.storage.api.dao.OrganizationDao;
import io.lumeer.storage.api.exception.ResourceNotFoundException;
import io.lumeer.storage.api.query.DatabaseQuery;
import io.lumeer.storage.hbase.model.HbaseOrganization;
import org.apache.hadoop.hbase.TableName;

import javax.enterprise.context.ApplicationScoped;
import java.io.IOException;
import java.util.List;

@ApplicationScoped
public class HbaseOrganizationDao extends SystemScopedDao implements OrganizationDao {


   @Override
   public Organization createOrganization(final Organization organization) {
      HbaseOrganization hbaseOrganization = new HbaseOrganization(organization);
      try {
         datastore.createDocument(TableName.valueOf(LumeerConst.Document.COLLECTION_NAME), hbaseOrganization.toDataDocument());
      } catch (IOException e) {
         e.printStackTrace();
      }
      return hbaseOrganization;
   }

   @Override
   public Organization updateOrganization(final String organizationId, final Organization organization) {
      HbaseOrganization hbaseOrganization = new HbaseOrganization(organization);
      hbaseOrganization.setId(organizationId);
      try {
         datastore.updateDocument(TableName.valueOf(LumeerConst.Document.COLLECTION_NAME), hbaseOrganization.toDataDocument());
      } catch (IOException e) {
         e.printStackTrace();
      }
      return hbaseOrganization;
   }

   @Override
   public void deleteOrganization(final String organizationId) {
      try {
         datastore.deleteDocument(TableName.valueOf(LumeerConst.Document.COLLECTION_NAME), organizationId);
      } catch (IOException e) {
         e.printStackTrace();
      }
   }

   @Override
   public Organization getOrganizationByCode(final String organizationCode) {
      Organization organization = null;
      try {
         organization = new HbaseOrganization(datastore.readDocument(TableName.valueOf(LumeerConst.Document.COLLECTION_NAME), "CODE", organizationCode));
      } catch (IOException e) {
         e.printStackTrace();
      }
      if (organization == null) {
         throw new ResourceNotFoundException(ResourceType.ORGANIZATION);
      }
      return organization;
   }

   @Override
   public List<Organization> getOrganizations(final DatabaseQuery query) {
      return null;
   }

}
