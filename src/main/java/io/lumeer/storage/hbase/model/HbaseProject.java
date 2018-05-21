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
package io.lumeer.storage.hbase.model;

import io.lumeer.api.model.Project;
import io.lumeer.storage.hbase.HBaseDataDocument;
import io.lumeer.storage.hbase.model.common.HbaseResource;
import io.lumeer.storage.hbase.model.embedded.HbasePermissions;
import org.mongodb.morphia.annotations.*;

import java.util.Map;

@Entity
@Indexes({
      @Index(fields = { @Field(value = HbaseProject.CODE) }, options = @IndexOptions(unique = true))
})
public class HbaseProject extends HbaseResource implements Project {

   public HbaseProject() {
   }

   public HbaseProject(Project project) {
      super(project);
   }

   @Override
   public Map<String, Object> toMap(){
      return super.toMap();
   }

   @Override
   public HBaseDataDocument toDataDocument(){
      return new HBaseDataDocument(toMap());
   }

   public HbaseProject(HBaseDataDocument doc){
      setId(doc.getString(ID));

      setCode(doc.getString(CODE));
      setName(doc.getString(NAME));
      setIcon(doc.getString(ICON));
      setName(doc.getString(COLOR));
      setPermissions((HbasePermissions) doc.get(PERMISSIONS));
   }

   @Override
   public String toString() {
      return "HbaseProject{" +
            "id=" + id +
            ", code='" + code + '\'' +
            ", name='" + name + '\'' +
            ", icon='" + icon + '\'' +
            ", color='" + color + '\'' +
            ", permissions=" + permissions +
            '}';
   }
}
