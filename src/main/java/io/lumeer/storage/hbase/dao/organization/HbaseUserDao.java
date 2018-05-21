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
import io.lumeer.api.model.ResourceType;
import io.lumeer.api.model.User;
import io.lumeer.storage.api.dao.UserDao;
import io.lumeer.storage.api.exception.ResourceNotFoundException;
import io.lumeer.storage.hbase.model.HbaseUser;
import org.apache.hadoop.hbase.TableName;

import javax.enterprise.context.RequestScoped;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

@RequestScoped
public class HbaseUserDao extends OrganizationScopedDao implements UserDao {

   private static final String PREFIX = "users_o-";

   @Override
   public void createUsersRepository(Organization organization) {
      try {
         datastore.createTable(databaseTable().toString());
      } catch (IOException e) {
         e.printStackTrace();
      }
   }

   @Override
   public void deleteUsersRepository(Organization organization) {
      try {
         datastore.deleteTable(databaseTable());
      } catch (IOException e) {
         e.printStackTrace();
      }
   }

   @Override
   public User createUser(final User user) {
      HbaseUser hbaseUser = new HbaseUser(user);
      try {
         datastore.createDocument(databaseTable(), hbaseUser.toDataDocument());
      } catch (IOException e) {
         e.printStackTrace();
      }
      return hbaseUser;
   }

   @Override
   public void updateUser(final String id, final User user) {
      HbaseUser hbaseUser = new HbaseUser(user);
      hbaseUser.setId(id);
      try {
         datastore.updateDocument(databaseTable(), hbaseUser.toDataDocument());
      } catch (IOException e) {
         e.printStackTrace();
      }
   }

   @Override
   public void deleteUser(final String id) {
      try {
         datastore.deleteDocument(databaseTable(), id);
      } catch (IOException e) {
         e.printStackTrace();
      }
   }

   @Override
   public Optional<User> getUserByUsername(final String username) {
      User user = null;
      try {
         user = new HbaseUser(datastore.readDocument(databaseTable(), "USERNAME", username));
      } catch (IOException e) {
         e.printStackTrace();
      }
      return Optional.ofNullable(user);
   }

   @Override
   public List<User> getAllUsers() {
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
