/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.compaction;

import java.util.Collections;
import java.util.function.LongPredicate;

import com.google.common.base.Throwables;
import com.google.common.collect.Sets;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableRewriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;

import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OutputHandler;

import static org.apache.cassandra.db.Keyspace.setInitialized;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

public class Downgrader
{
    private final ColumnFamilyStore cfs;
    private final SSTableReader sstable;
    private final LifecycleTransaction transaction;
    private final File directory;

    private final CompactionController controller;
    private final CompactionStrategyManager strategyManager;
    private final long estimatedRows;

    private final OutputHandler outputHandler;

    private final String version;

    public Downgrader(String version, ColumnFamilyStore cfs, LifecycleTransaction txn, OutputHandler outputHandler)
    {
        this.cfs = cfs;
        this.transaction = txn;
        this.sstable = txn.onlyOne();
        this.outputHandler = outputHandler;

        this.version = version;

        this.directory = new File(sstable.getFilename()).parent();

        this.controller = new UpgradeController(cfs);

        this.strategyManager = cfs.getCompactionStrategyManager();
        long estimatedTotalKeys = Math.max(cfs.metadata().params.minIndexInterval, SSTableReader.getApproximateKeyCount(Collections.singletonList(this.sstable)));
        long estimatedSSTables = Math.max(1, SSTableReader.getTotalBytes(Collections.singletonList(this.sstable)) / strategyManager.getMaxSSTableBytes());
        this.estimatedRows = (long) Math.ceil((double) estimatedTotalKeys / estimatedSSTables);
    }

    public Downgrader(String version, ColumnFamilyStore cfs, OutputHandler outputHandler)
    {
        this.cfs = cfs;

        this.outputHandler = outputHandler;

        this.version = version;

        this.directory = cfs.getDirectories().getDirectoryForNewSSTables();

        this.controller = new UpgradeController(cfs);

        this.strategyManager = cfs.getCompactionStrategyManager();

        this.transaction = null;

        this.sstable = null;

        this.estimatedRows = 0L;
    }

    private SSTableWriter createCompactionWriter(StatsMetadata metadata)
    {
        MetadataCollector sstableMetadataCollector = new MetadataCollector(cfs.getComparator());
        sstableMetadataCollector.sstableLevel(sstable.getSSTableLevel());



        outputHandler.output("=======================================++> cfs: " + cfs.toString());
        TableMetadata metadataRef = cfs.metadata();


        outputHandler.output("=======================================++> metadataRef: " + metadataRef.toString());

       // newSSTableDescriptor(directory, DatabaseDescriptor.getSelectedSSTableFormat().getLatestVersion())

        Descriptor descriptor = cfs.newSSTableDescriptor(directory, DatabaseDescriptor.getSelectedSSTableFormat().getVersion(version));
        //Descriptor descriptor = cfs.newSSTableDescriptor(directory, new BigFormat.BigVersion(BigFormat.getInstance()version));
        return descriptor.getFormat().getWriterFactory().builder(descriptor)
                         .setKeyCount(estimatedRows)
                         .setRepairedAt(metadata.repairedAt)
                         .setPendingRepair(metadata.pendingRepair)
                         .setTransientSSTable(metadata.isTransient)
                         .setTableMetadataRef(cfs.metadata)
                         .setMetadataCollector(sstableMetadataCollector)
                         .setSerializationHeader(SerializationHeader.make(cfs.metadata(), Sets.newHashSet(sstable)))
                         .addDefaultComponents(cfs.indexManager.listIndexGroups())
                         .setSecondaryIndexGroups(cfs.indexManager.listIndexGroups())
                         .build(transaction, cfs);
    }


//    private ColumnFamilyStore removeColumn(ColumnFamilyStore cfs, String columnName) {
//        String keyspaceName = cfs.keyspace.getName();
//        String tableName = cfs.getTableName();
//
//        // Execute the ALTER TABLE statement to drop the column
//        String query = String.format("ALTER TABLE %s.%s DROP %s", keyspaceName, tableName, columnName);
//        QueryProcessor.process(query, ConsistencyLevel.ALL);
//
//        // Get and return the updated ColumnFamilyStore
//        return Keyspace.open(keyspaceName).getColumnFamilyStore(tableName);
//    }

    public void downgrade(boolean keepOriginals)
    {
        outputHandler.output("Downgrade " + sstable);
        long nowInSec = FBUtilities.nowInSeconds();

//        if (cfs.metadata.name.equals("compaction_history")) {
//            // Remove the compaction_properties column
//            // Note: You need to implement the removeColumn method
//            cfs = removeColumn(cfs, "compaction_properties");
//        }
        setInitialized();
        try (SSTableRewriter writer = SSTableRewriter.construct(cfs, transaction, keepOriginals, CompactionTask.getMaxDataAge(transaction.originals()));
             AbstractCompactionStrategy.ScannerList scanners = strategyManager.getScanners(transaction.originals());
             CompactionIterator iter = new CompactionIterator(transaction.opType(), scanners.scanners, controller, nowInSec, nextTimeUUID()))
        {
            writer.switchWriter(createCompactionWriter(sstable.getSSTableMetadata()));
            iter.setTargetDirectory(writer.currentWriter().getFilename());
            while (iter.hasNext())
                writer.append(iter.next());

            writer.finish();
            outputHandler.output("Downgrade of " + sstable + " complete.");
        }
        catch (Exception e)
        {
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
        finally
        {
            controller.close();
        }
    }


    public void removeExtracColumn() throws Exception
    {
        try(CQLSSTableWriter droppedColumnsWriter = writerFor("dropped_columns"))
        {
            writerDroppedColumns(droppedColumnsWriter, tableMetadata(DropColumns, "dropped_columns"));

        }
    }

    private static class UpgradeController extends CompactionController
    {
        public UpgradeController(ColumnFamilyStore cfs)
        {
            super(cfs, Integer.MAX_VALUE);
        }

        @Override
        public LongPredicate getPurgeEvaluator(DecoratedKey key)
        {
            return time -> false;
        }
    }

public static final String DroppedColumnInsert = "INSERT INTO system_schema_v4.dropped_columns (keyspace_name, table_name, column_name, dropped_time, kind, type) VALUES (?, ?, ?, ?, ?, ?)";

private void writerDroppedColumns(CQLSSTableWriter droppedColumnsWriter, TableMetadata tableMetadata) throws Exception
    {
        droppedColumnsWriter.addRow("system", "compaction_history", "compaction_properties", new  java.util.Date(Clock.Global.currentTimeMillis()), "regular", "frozen<map<text, text>>");
    }


    private static TableId tableIdOf(String table){

        return Schema.instance.getKeyspaceMetadata("system_schema").getTableOrViewNullable(table).id;
    }

    private static final String DropColumns =

                    "CREATE TABLE %s.dropped_columns (" +
                    "keyspace_name text," +
                    "table_name text," +
                    "column_name text," +
                    "dropped_time timestamp," +
                    "kind text," +
                    "type text," +
                    "PRIMARY KEY (keyspace_name, table_name, column_name))";




    private static TableMetadata tableMetadata(String schema, String table){
        CreateTableStatement.Raw schemaStatement = QueryProcessor.parseStatement(String.format(schema, "system_schema_v4"), CreateTableStatement.Raw.class, "CREATE TABLE");
        ClientState state = ClientState.forInternalCalls();
        CreateTableStatement statement = schemaStatement.prepare(state);
        statement.validate(ClientState.forInternalCalls());

        TableMetadata.Builder builder = statement.builder(org.apache.cassandra.schema.Types.rawBuilder("system_schema_v4").build());
        return builder
               .id(tableIdOf(table))
               .build();

    }

    private CQLSSTableWriter writerFor(String table){

        return CQLSSTableWriter.builder()
                              .inDirectory(directory)
                              .forTable(String.format(DropColumns, "system_schema_v4"))
                              .using(DroppedColumnInsert)
                              .build();
    }
}

