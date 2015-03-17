/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseTest {
  private static final Logger log = LoggerFactory.getLogger(HBaseTest.class);

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration(false);
    conf.addResource("/usr/local/lib/hadoop/etc/hadoop/core-site.xml");
    conf.addResource("/usr/local/lib/hadoop/etc/hadoop/hdfs-site.xml");
    conf.addResource("/usr/local/lib/hbase/conf/hbase-site.xml");

    final boolean WRITE_DATA = true, READ_DATA = true;
    final int NUM_ROWS = 1000 * 1000;
    final int NUM_COLS = 10;
    long entriesWritten = 0;
    final byte[] cf = "cf".getBytes();

    Connection conn = ConnectionFactory.createConnection(conf);
    Admin admin = conn.getAdmin();
    Set<TableName> tables = new HashSet<>(Arrays.asList(admin.listTableNames()));
    TableName tableName = TableName.valueOf("test");
    if (!tables.contains(tableName)) {
      HTableDescriptor tableDesc = new HTableDescriptor(tableName);
      tableDesc.addFamily(new HColumnDescriptor(cf));
      byte[][] splits = new byte[9][2];
      for (int i = 1; i < 10; i++) {
        int split = 48 + i;
        splits[i - 1][0] = (byte) (split >>> 8);
        splits[i - 1][0] = (byte) (split);
      }
      admin.createTable(tableDesc, splits);
    }

    try (Table table = conn.getTable(TableName.valueOf("test"))) {

      if (WRITE_DATA) {
        System.out.println("Write buffer size: " + table.getWriteBufferSize());

        List<Put> puts = new ArrayList<>();
        for (int i = 0; i < NUM_ROWS; i++) {
          Put p = new Put(Integer.toString(i).getBytes());
          for (int j = 0; j < NUM_COLS; j++) {
            byte[] value = new byte[50];
            Bytes.random(value);
            p.addColumn(cf, Integer.toString(j).getBytes(), value);
          }
          puts.add(p);

          // Flush the writes
          if (puts.size() == 1000) {
            Object[] results = new Object[1000];
            try {
              table.batch(puts, results);
            } catch (IOException e) {
              log.error("Failed to write data", e);
              log.info("Errors: {}", Arrays.toString(results));
            }
            entriesWritten += puts.size();
            puts.clear();

            if (entriesWritten % 50000 == 0) {
              log.info("Wrote {} entries", entriesWritten);
            }
          }
        }

        if (puts.size() > 0) {
          entriesWritten += puts.size();
          Object[] results = new Object[puts.size()];
          try {
            table.batch(puts, results);
          } catch (IOException e) {
            log.error("Failed to write data", e);
            log.info("Error: {}", Arrays.toString(results));
          }
        }

        log.info("Wrote {} entries in total", entriesWritten);
      }

      if (READ_DATA) {
        long rowsObserved = 0l;
        long entriesObserved = 0l;
        ResultScanner scanner = table.getScanner(new Scan());
        String row;
        for (Result result : scanner) {
          rowsObserved++;
          row = new String(result.getRow());
          if (rowsObserved % 1000 == 0) {
            log.info("Saw row {}", row);
          }
          CellScanner cells = result.cellScanner();
          while (cells.advance()) {
            entriesObserved++;
            if (entriesObserved % 100000 == 0) {
              Cell cell = cells.current();
              String family = new String(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
              String qualifier = new String(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
              log.info(row + " " + family + ":" + qualifier + " value[" + (cell.getValueLength() - cell.getValueOffset()) + "]");
            }
          }
          log.info("Last row {}", row);
        }

        log.info("Saw {} rows", rowsObserved);
        log.info("Saw {} cells", entriesObserved);
      }

      log.info(table.get(new Get("999997".getBytes())).toString());
      log.info(table.get(new Get("999998".getBytes())).toString());
      log.info(table.get(new Get("999999".getBytes())).toString());
    }
  }
}
