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
package io.lumeer.storage.hbase.model.common;

import io.lumeer.storage.hbase.HBaseDataDocument;
import org.bson.types.ObjectId;
import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.annotations.Version;

import java.util.HashMap;
import java.util.Map;

public abstract class HbaseEntity {

   public static final String ID = "_id";
   public static final String VERSION = "version";

   @Id
   protected ObjectId id;

   @Version(VERSION)
   private Long version;

   protected HbaseEntity() {
   }

   public Map<String, Object> toMap(){
      HashMap<String, Object> map = new HashMap<>();
      if (getId() != null) map.put(ID, getId());
      return map;
   }

   public HBaseDataDocument toDataDocument(){
      return new HBaseDataDocument(toMap());
   }

   protected HbaseEntity(final String id) {
      this.id = id != null ? new ObjectId(id) : null;
   }

   public String getId() {
      return id != null ? id.toHexString() : null;
   }

   public void setId(final String id) {
      this.id = new ObjectId(id);
   }
}
