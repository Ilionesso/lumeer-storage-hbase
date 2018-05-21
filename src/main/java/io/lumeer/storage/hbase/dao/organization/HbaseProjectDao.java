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
package io.lumeer.storage.hbase.dao.organization;

import io.lumeer.api.model.Organization;
import io.lumeer.api.model.Project;
import io.lumeer.api.model.ResourceType;
import io.lumeer.storage.api.dao.ProjectDao;
import io.lumeer.storage.api.exception.ResourceNotFoundException;
import io.lumeer.storage.api.query.DatabaseQuery;
import io.lumeer.storage.hbase.model.HbaseProject;
import org.apache.hadoop.hbase.TableName;

import javax.enterprise.context.RequestScoped;
import java.io.IOException;
import java.util.List;

@RequestScoped
public class HbaseProjectDao extends OrganizationScopedDao implements ProjectDao {

   private static final String PREFIX = "projects_o-";


   @Override
   public void createProjectsRepository(Organization organization) {
      try {
         datastore.createTable(databaseTable().toString());
      } catch (IOException e) {
         e.printStackTrace();
      }
   }

   @Override
   public void deleteProjectsRepository(Organization organization) {
      try {
         datastore.deleteTable(databaseTable());
      } catch (IOException e) {
         e.printStackTrace();
      }
   }

   @Override
   public Project createProject(final Project project) {

      HbaseProject hbaseProject = new HbaseProject(project);
      try {
         datastore.createDocument(databaseTable(), hbaseProject.toDataDocument());
      } catch (IOException e) {
         e.printStackTrace();
      }
      return hbaseProject;
   }

   @Override
   public Project updateProject(final String projectId, final Project project) {
      HbaseProject hbaseProject = new HbaseProject(project);
      hbaseProject.setId(projectId);
      try {
         datastore.updateDocument(databaseTable(), hbaseProject.toDataDocument());
      } catch (IOException e) {
         e.printStackTrace();
      }
      return hbaseProject;
   }

   @Override
   public void deleteProject(final String projectId) {
      try {
         datastore.deleteDocument(databaseTable(), projectId);
      } catch (IOException e) {
         e.printStackTrace();
      }
   }

   @Override
   public Project getProjectByCode(final String projectCode) {
      HbaseProject project = null;
      try {
         project = new HbaseProject(datastore.readDocument(databaseTable(), "CODE", projectCode));
      } catch (IOException e) {
         e.printStackTrace();
      }
      if (project == null) {
         throw new ResourceNotFoundException(ResourceType.PROJECT);
      }
      return project;
   }

   @Override
   public List<Project> getProjects(DatabaseQuery query) {
      return null;
   }


   TableName databaseTable() {
      if (!getOrganization().isPresent()) {
         throw new ResourceNotFoundException(ResourceType.ORGANIZATION);
      }
      return databaseTable(getOrganization().get());
   }

   private TableName databaseTable(Organization organization) {
      return TableName.valueOf(PREFIX + organization.getId());
   }



}
