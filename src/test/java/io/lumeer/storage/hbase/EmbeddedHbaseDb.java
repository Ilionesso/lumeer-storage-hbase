package io.lumeer.storage.hbase;/*
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;

import java.io.IOException;


public class EmbeddedHbaseDb {

   private Configuration config;
   private Admin admin;
   private Connection connection;
   private AggregationClient aggregationClient;

   public EmbeddedHbaseDb() {
   }


   public void start() throws IOException, ServiceException {
      config = HBaseConfiguration.create();

      String path = this.getClass()
              .getClassLoader()
              .getResource("hbase-site.xml")
              .getPath();
      config.addResource(new Path(path));
      connection = ConnectionFactory.createConnection(config);
      admin = connection.getAdmin();
      aggregationClient = new AggregationClient(config);
      HBaseAdmin.checkHBaseAvailable(config);
   }

   public void stop() throws IOException {
      if (admin != null){
//         admin.shutdown();
         admin.close();
      }
   }

   public Configuration getConfig() {
      return config;
   }

   public Admin getAdmin() {
      return admin;
   }

   public Connection getConnection() {
      return connection;
   }

   public AggregationClient getAggregationClient() {
      return aggregationClient;
   }
}
