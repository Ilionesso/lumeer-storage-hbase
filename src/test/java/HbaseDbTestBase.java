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

import com.google.protobuf.ServiceException;
import io.lumeer.storage.hbase.HBaseStorageAdapter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;

public abstract class HbaseDbTestBase {

   private static EmbeddedHbaseDb embeddedHbaseDb;
   protected static HBaseStorageAdapter hBaseStorageAdapter;

   private Configuration config;
   private Admin admin;
   private Connection connection;
   private AggregationClient aggregationClient;

//   static {
//      morphia.getMapper().getOptions().setStoreEmpties(true);
//   }


   @BeforeClass
   public static void startEmbeddedHbaseDb() throws IOException, ServiceException {
      embeddedHbaseDb = new EmbeddedHbaseDb();
      embeddedHbaseDb.start();
      hBaseStorageAdapter = new HBaseStorageAdapter();
      hBaseStorageAdapter.connect();
   }

   @AfterClass
   public static void stopEmbeddedHbaseDb() throws IOException {
      hBaseStorageAdapter.disconnect();
      if (embeddedHbaseDb != null) {
         embeddedHbaseDb.stop();
      }
   }

   @Before
   public void connectMongoDbStorage() {

//      database.drop();
   }

   @After
   public void disconnectMongoDbStorage() {
//      if (hBaseStorageAdapter != null) {
//         hBaseStorageAdapter.disconnect();
//      }
   }
}
