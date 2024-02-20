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

package org.apache.cassandra.tools;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.Downgrader;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableIdFactory;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.OutputHandler;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_UTIL_ALLOW_TOOL_REINIT_FOR_TEST;
import static org.apache.cassandra.index.internal.CassandraIndex.indexCfsMetadata;
import static org.apache.cassandra.tools.BulkLoader.CmdLineOptions;
public class StandaloneDowngrader
{
    private static final String TOOL_NAME = "sstabledowngrade";
    private static final String DEBUG_OPTION = "debug";
    private static final String HELP_OPTION = "help";

    private static final String CASSANDRA_4_VERSION = "nb";

    public static void main(String args[])
    {
        Options options = Options.parseArgs(args);
        if (TEST_UTIL_ALLOW_TOOL_REINIT_FOR_TEST.getBoolean())
            DatabaseDescriptor.toolInitialization(false); //Necessary for testing
        else
            Util.initDatabaseDescriptor();

        try
        {
            // load keyspace descriptions.
            Schema.instance.loadFromDisk();

            if (Schema.instance.getTableMetadataRef(options.keyspace, options.cf) == null)
                throw new IllegalArgumentException(String.format("Unknown keyspace/table %s.%s",
                                                                 options.keyspace,
                                                                 options.cf));

            Keyspace keyspace = Keyspace.openWithoutSSTables(options.keyspace);

            System.out.println(String.format("Downgrading sstables for %s.%s", options.keyspace, options.cf));
            ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(options.cf);

            OutputHandler handler = new OutputHandler.SystemOutput(false, options.debug);

            Directories.SSTableLister lister = cfs.getDirectories().sstableLister(Directories.OnTxnErr.THROW);


            if (options.snapshot != null)
                lister.onlyBackups(true).snapshots(options.snapshot);
            else
                lister.includeBackups(false);

            Collection<SSTableReader> readers = new ArrayList<>();

            // Downgrade sstables in id order
            for (Map.Entry<Descriptor, Set<Component>> entry : lister.sortedList())
            {

                Set<Component> components = entry.getValue();
                if (!components.containsAll(entry.getKey().getFormat().primaryComponents()))
                    continue;

                try
                {
                    SSTableReader sstable = SSTableReader.openNoValidation(entry.getKey(), components, cfs);
                    if (sstable.descriptor.version.equals(DatabaseDescriptor.getSelectedSSTableFormat().getLatestVersion()))
                    {
                        readers.add(sstable);
                        continue;
                    }

                    sstable.selfRef().release();
                }
                catch (Exception e)
                {
                    JVMStabilityInspector.inspectThrowable(e);
                    System.err.println(String.format("Error Loading %s: %s", entry.getKey(), e.getMessage()));
                    if (options.debug)
                        e.printStackTrace(System.err);
                }
            }

            int numSSTables = readers.size();
            handler.output("Found " + numSSTables + " sstables that need downgrading.");

            for (SSTableReader sstable : readers)
            {
                try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.DOWNGRADE_SSTABLES, sstable))
                {
                    Downgrader downgrader = new Downgrader(CASSANDRA_4_VERSION, cfs, txn, handler);
                    downgrader.downgrade(options.keepSource);
                }
                catch (Exception e)
                {
                    System.err.println(String.format("Error downgrading %s: %s", sstable, e.getMessage()));
                    if (options.debug)
                        e.printStackTrace(System.err);
                }
                finally
                {
                    // we should have released this through commit of the LifecycleTransaction,
                    // but in case the downgrade failed (or something else went wrong) make sure we don't retain a reference
                    sstable.selfRef().ensureReleased();
                }
            }

            Collection<Index> indexes = cfs.indexManager.listIndexes();
            Collection<SSTableReader> sstablesIdx = new ArrayList<>();
            for (Index idx : indexes)
            {

                TableMetadataRef tableRef = TableMetadataRef.forOfflineTools(indexCfsMetadata(cfs.metadata(), idx.getIndexMetadata()));
                Directories directories = new Directories(tableRef.get());

                Directories.SSTableLister sstableFiles = directories.sstableLister(Directories.OnTxnErr.IGNORE).skipTemporary(true);


                ColumnFamilyStore idxcf = new ColumnFamilyStore(keyspace, tableRef.name,
                                                                directories.getUIDGenerator(SSTableIdFactory.instance.defaultBuilder()),
                                                                tableRef, directories, true, false, true);
                sstablesIdx = SSTableReader.openAll(cfs, sstableFiles.list().entrySet(), tableRef);

                deleteObsoleteSSTables(idxcf, sstablesIdx);

            }


                // Get all SSTables
                Collection<SSTableReader> sstables = cfs.getLiveSSTables();


                // Filter out the obsolete SSTables
                List<SSTableReader> obsoleteSSTables = sstables.stream()
                                                               .filter(SSTableReader::isMarkedCompacted)
                                                               .collect(Collectors.toList());


                // Delete the obsolete SSTables
                try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.UNKNOWN, obsoleteSSTables))
                {
                    if (txn == null)
                        throw new IllegalStateException("Failed to mark SSTables as compacting");

                    for (SSTableReader sstable : obsoleteSSTables)
                    {
                        sstable.selfRef().release();
                    }

                    txn.finish();
                }

            CompactionManager.instance.finishCompactionsAndShutdown(5, TimeUnit.MINUTES);
            LifecycleTransaction.waitForDeletions();
            System.exit(0);
        }
        catch (Exception e)
        {
            System.err.println(e.getMessage());
            if (options.debug)
                e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    private static class Options
    {
        public final String keyspace;
        public final String cf;
        public final String snapshot;

        public boolean debug;
        public boolean keepSource;

        private Options(String keyspace, String cf, String snapshot)
        {
            this.keyspace = keyspace;
            this.cf = cf;
            this.snapshot = snapshot;
        }

        public static Options parseArgs(String cmdArgs[])
        {
            CommandLineParser parser = new GnuParser();
            CmdLineOptions options = getCmdLineOptions();
            try
            {
                CommandLine cmd = parser.parse(options, cmdArgs, false);

                if (cmd.hasOption(HELP_OPTION))
                {
                    printUsage(options);
                    System.exit(0);
                }

                String[] args = cmd.getArgs();
                if (args.length >= 4 || args.length < 2)
                {
                    String msg = args.length < 2 ? "Missing arguments" : "Too many arguments";
                    errorMsg(msg, options);
                    System.exit(1);
                }

                String keyspace = args[0];
                String cf = args[1];
                String snapshot = null;
                if (args.length == 3)
                    snapshot = args[2];

                Options opts = new Options(keyspace, cf, snapshot);

                opts.debug = cmd.hasOption(DEBUG_OPTION);

                return opts;
            }
            catch (ParseException e)
            {
                errorMsg(e.getMessage(), options);
                return null;
            }
        }

        private static void errorMsg(String msg, CmdLineOptions options)
        {
            System.err.println(msg);
            printUsage(options);
            System.exit(1);
        }

        private static CmdLineOptions getCmdLineOptions()
        {
            CmdLineOptions options = new CmdLineOptions();
            options.addOption(null, DEBUG_OPTION, "display stack traces");
            options.addOption("h", HELP_OPTION, "display this help message");

            return options;
        }

        public static void printUsage(CmdLineOptions options)
        {
            String usage = String.format("%s [options] <keyspace> <cf> [snapshot]", TOOL_NAME);
            StringBuilder header = new StringBuilder();
            header.append("--\n");
            header.append("Downgrade the sstables in the given cf (or snapshot) to the current version of Cassandra.");
            header.append("This operation will rewrite the sstables in the specified cf to match the ");
            header.append("previous installed version of Cassandra.\n");
            header.append("The snapshot option will only downgrade the specified snapshot.");
            header.append("\n--\n");
            header.append("Options are:");
            new HelpFormatter().printHelp(usage, header.toString(), options, "");
        }
    }

    public static void deleteObsoleteSSTables(ColumnFamilyStore cfs, Collection<SSTableReader> sstables)
    {
        Directories directories = cfs.getDirectories();

        List<File> driec = directories.getCFDirectories();
        for (File dri : driec)
        {

            try {
                // Use Files.walk to iterate over each file in the directory
                Files.walk(dri.toPath())
                     .filter(Files::isRegularFile) // Filter out directories, we only want files
                     .filter(path -> path.getFileName().toString().contains("oa")) // Filter files starting with "oa_"
                     .forEach(path -> {
                         try {
                             Files.delete(path); // Delete the file
                         } catch (IOException e) {
                             e.printStackTrace();
                         }
                     });
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // Filter and delete obsolete SSTables
        for (SSTableReader sstable : sstables)
        {
            if (!sstable.descriptor.version.equals(DatabaseDescriptor.getSelectedSSTableFormat().getLatestVersion()))
            {
                continue;
            }


            // Get all components of the SSTable
            Set<Component> components = sstable.getComponents();

            // Delete all components
            for (Component component : components)
            {
                File componentFile = new File(String.valueOf(sstable.descriptor.fileFor(component)));
                if (componentFile.exists())
                {
                    FileUtils.delete(componentFile);
                }
            }


        }





    }

}
