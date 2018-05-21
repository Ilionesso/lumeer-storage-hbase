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

import io.lumeer.api.model.Attribute;
import io.lumeer.api.model.Collection;
import io.lumeer.storage.hbase.HBaseDataDocument;
import io.lumeer.storage.hbase.model.common.HbaseResource;
import io.lumeer.storage.hbase.model.embedded.HbaseAttribute;
import io.lumeer.storage.hbase.model.embedded.HbasePermissions;
import org.mongodb.morphia.annotations.*;
import org.mongodb.morphia.utils.IndexType;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

@Entity
@Indexes({
      @Index(fields = { @Field(HbaseCollection.CODE) }, options = @IndexOptions(unique = true)),
      @Index(fields = { @Field(HbaseCollection.NAME) }, options = @IndexOptions(unique = true)),
      @Index(fields = { @Field(HbaseCollection.ATTRIBUTES + "." + HbaseAttribute.NAME) }),
      @Index(fields = {
            @Field(value = HbaseCollection.CODE, type = IndexType.TEXT),
            @Field(value = HbaseCollection.NAME, type = IndexType.TEXT),
            @Field(value = HbaseCollection.ATTRIBUTES + "." + HbaseAttribute.NAME, type = IndexType.TEXT)
      })
})
public class HbaseCollection extends HbaseResource implements Collection {

   public static final String ATTRIBUTES = "attributes";
   public static final String DOCUMENTS_COUNT = "docCount";
   public static final String LAST_TIME_USED = "lastTimeUsed";

   @Embedded(ATTRIBUTES)
   private Set<HbaseAttribute> attributes;

   @Property(DOCUMENTS_COUNT)
   private Integer documentsCount;

   @Property(LAST_TIME_USED)
   private LocalDateTime lastTimeUsed;

   public HbaseCollection() {
   }

   public HbaseCollection(Collection collection) {
      super(collection);

      this.attributes = HbaseAttribute.convert(collection.getAttributes());
      this.documentsCount = collection.getDocumentsCount();
      this.lastTimeUsed = collection.getLastTimeUsed();
   }

   @Override
   public Map<String, Object> toMap(){
      Map<String, Object> map = super.toMap();
      if (getAttributes() != null) map.put(ATTRIBUTES, getAttributes());
      if (getDocumentsCount() != null) map.put(DOCUMENTS_COUNT, getDocumentsCount());
      if (getLastTimeUsed() != null) map.put(LAST_TIME_USED, getLastTimeUsed());
      return map;
   }

   @Override
   public HBaseDataDocument toDataDocument(){
      return new HBaseDataDocument(toMap());
   }

   public HbaseCollection(HBaseDataDocument doc){
      setCode(doc.getString(CODE));
      setName(doc.getString(NAME));
      setIcon(doc.getString(ICON));
      setName(doc.getString(COLOR));
      setPermissions((HbasePermissions) doc.get(PERMISSIONS));
   }

   @Override
   public Set<Attribute> getAttributes() {
      return Collections.unmodifiableSet(attributes);
   }

   public void setAttributes(final Set<Attribute> attributes) {
      this.attributes = HbaseAttribute.convert(attributes);
   }

   @Override
   public void updateAttribute(final String attributeFullName, final Attribute attribute) {
      deleteAttribute(attributeFullName);
      attributes.add(HbaseAttribute.convert(attribute));
   }

   @Override
   public void deleteAttribute(final String attributeFullName) {
      attributes.removeIf(a -> a.getFullName().equals(attributeFullName));
   }

   @Override
   public Integer getDocumentsCount() {
      return documentsCount;
   }

   public void setDocumentsCount(final Integer documentsCount) {
      this.documentsCount = documentsCount;
   }

   @Override
   public LocalDateTime getLastTimeUsed() {
      return lastTimeUsed;
   }

   @Override
   public void setLastTimeUsed(final LocalDateTime lastTimeUsed) {
      this.lastTimeUsed = lastTimeUsed;
   }

   @Override
   public boolean equals(final Object o) {
      if (this == o) {
         return true;
      }
      if (!(o instanceof Collection)) {
         return false;
      }

      final Collection that = (Collection) o;

      return getCode() != null ? getCode().equals(that.getCode()) : that.getCode() == null;
   }

   @Override
   public int hashCode() {
      return getCode() != null ? getCode().hashCode() : 0;
   }

   @Override
   public String toString() {
      return "HbaseCollection{" +
            "id='" + id + '\'' +
            ", code='" + code + '\'' +
            ", name='" + name + '\'' +
            ", icon='" + icon + '\'' +
            ", color='" + color + '\'' +
            ", permissions=" + permissions +
            ", attributes=" + attributes +
            ", documentsCount=" + documentsCount +
            ", lastTimeUsed=" + lastTimeUsed +
            '}';
   }
}
