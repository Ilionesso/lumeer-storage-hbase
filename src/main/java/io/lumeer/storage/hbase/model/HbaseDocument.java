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

import io.lumeer.api.model.Document;
import io.lumeer.engine.api.data.DataDocument;
import io.lumeer.storage.hbase.HBaseDataDocument;
import io.lumeer.storage.hbase.model.common.HbaseEntity;
import org.mongodb.morphia.annotations.*;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.Map;

@Entity
@Indexes({
      @Index(fields = @Field(HbaseDocument.COLLECTION_ID))
})
public class HbaseDocument extends HbaseEntity implements Document {

   public static final String COLLECTION_ID = "collectionId";
   public static final String CREATION_DATE = "creationDate";
   public static final String UPDATE_DATE = "updateDate";
   public static final String CREATED_BY = "createdBy";
   public static final String UPDATED_BY = "updatedBy";
   public static final String DATA_VERSION = "dataVersion";

   @Property(COLLECTION_ID)
   private String collectionId;

   @Transient
   private String collectionCode;

   @Property(CREATION_DATE)
   private LocalDateTime creationDate;

   @Property(UPDATE_DATE)
   private LocalDateTime updateDate;

   @Property(CREATED_BY)
   private String createdBy;

   @Property(UPDATED_BY)
   private String updatedBy;

   @Property(DATA_VERSION)
   private Integer dataVersion;

   @Transient
   private DataDocument data;

   public HbaseDocument() {
   }

   public HbaseDocument(Document document) {
      super(document.getId());

      this.collectionId = document.getCollectionId();
      this.collectionCode = document.getCollectionCode();
      this.creationDate = document.getCreationDate();
      this.updateDate = document.getUpdateDate();
      this.createdBy = document.getCreatedBy();
      this.updatedBy = document.getUpdatedBy();
      this.dataVersion = document.getDataVersion();
      this.data = document.getData();
   }

   @Override
   public Map<String, Object> toMap(){
      Map<String, Object> map = super.toMap();
      if (getCollectionId() != null) map.put(COLLECTION_ID, getCollectionId());
      if (getCollectionCode() != null) map.put("COLLECTION_CODE", getCollectionCode());
      if (getCreationDate() != null) map.put(CREATION_DATE, getCreationDate());
      if (getUpdateDate() != null) map.put(UPDATE_DATE, getUpdateDate());
      if (getCreatedBy() != null) map.put(CREATED_BY, getCreatedBy());
      if (getUpdatedBy() != null) map.put(UPDATED_BY, getUpdatedBy());
      if (getDataVersion() != null) map.put(CREATED_BY, getDataVersion());
      if (getData() != null) map.put("DATA", getData());

      return map;
   }

   public HbaseDocument(HBaseDataDocument doc){
      setId(doc.getString(ID));
      setCollectionCode(doc.getString("COLLECTION_CODE"));
      setCollectionId(doc.getString(COLLECTION_ID));
      setCreatedBy(doc.getString(CREATED_BY));
      setCreationDate((LocalDateTime) doc.get(CREATION_DATE));
      setUpdateDate((LocalDateTime) doc.get(UPDATE_DATE));
   }

   @Override
   public HBaseDataDocument toDataDocument(){
      return new HBaseDataDocument(toMap());
   }

   @Override
   public String getCollectionId() {
      return collectionId;
   }

   @Override
   public void setCollectionId(final String collectionId) {
      this.collectionId = collectionId;
   }

   @Override
   public String getCollectionCode() {
      return collectionCode;
   }

   @Override
   public void setCollectionCode(final String collectionCode) {
      this.collectionCode = collectionCode;
   }

   @Override
   public LocalDateTime getCreationDate() {
      return creationDate;
   }

   @Override
   public void setCreationDate(final LocalDateTime creationDate) {
      this.creationDate = creationDate;
   }

   @Override
   public LocalDateTime getUpdateDate() {
      return updateDate;
   }

   @Override
   public void setUpdateDate(final LocalDateTime updateDate) {
      this.updateDate = updateDate;
   }

   @Override
   public String getCreatedBy() {
      return createdBy;
   }

   @Override
   public void setCreatedBy(final String createdBy) {
      this.createdBy = createdBy;
   }

   @Override
   public String getUpdatedBy() {
      return updatedBy;
   }

   @Override
   public void setUpdatedBy(final String updatedBy) {
      this.updatedBy = updatedBy;
   }

   @Override
   public Integer getDataVersion() {
      return dataVersion;
   }

   @Override
   public void setDataVersion(final Integer dataVersion) {
      this.dataVersion = dataVersion;
   }

   @Override
   public DataDocument getData() {
      return data;
   }

   @Override
   public void setData(final DataDocument data) {
      this.data = data;
   }

   @Override
   public boolean equals(final Object o) {
      if (this == o) {
         return true;
      }
      if (!(o instanceof Document)) {
         return false;
      }

      final Document document = (Document) o;

      return getId() != null ? getId().equals(document.getId()) : document.getId() == null;
   }

   @Override
   public int hashCode() {
      return getId() != null ? getId().hashCode() : 0;
   }

   @Override
   public String toString() {
      return "HbaseDocument{" +
            "id=" + id +
            ", collectionId='" + collectionId + '\'' +
            ", collectionCode='" + collectionCode + '\'' +
            ", creationDate=" + creationDate +
            ", updateDate=" + updateDate +
            ", createdBy='" + createdBy + '\'' +
            ", updatedBy='" + updatedBy + '\'' +
            ", dataVersion=" + dataVersion +
            ", data=" + data +
            '}';
   }


   private Date convertLocalDateTimeToDate(LocalDateTime dateTime) {
      if (dateTime == null) {
         return null;
      }
      return Date.from(dateTime.atZone(ZoneId.systemDefault()).toInstant());
   }
}
