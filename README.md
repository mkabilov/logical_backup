## Introduction

The purpose of the Logical Backup Tool (LBT) is to create a continuous
incremental logical backup of one or more PostgreSQL database tables. For each
table that should be backed up, the tool stores a continuous stream of changes
(called deltas) in files on disk; if necessary, it also produces a complete SQL
dump of the table. As opposed to the physical backup, i.e. `pg_basebackup` the
outcome is not a page-by-page copy of the target cluster and cannot be used in
situations that expect the exact full copy, e.g., when creating a physical
replica. At the same time, it doesn't have the limitations of the physical copy
and can be used to restore target tables to a system running a different
PostgreSQL major version from the original one, as well as to those running on
an incompatible CPU architecture.

The primary use-case for the tool is protection against a data-loss of one or
more tables in an OLAP environment. Those are environments with high volumes of
data (typically in the range of tens or hundreds of TB) and relatively low
frequency of changes, possibly just a few thousand rows per day. Usually, the
analytical data stored in the database is generated from some primary source;
therefore, slight inconsistencies between different tables are tolerated and the
goal, when performing the data recovery, is to restore some target tables to the
latest state stored in the backup one by one, as opposed to restoring all
dataset at once to a consistent point in time. Therefore, the tool will not
guarantee that cross-table constraints, such as foreign keys, will be satisfied
as a result of the restore, and it could be necessary to remove those
constraints from the dataset being backed up. In other words, the restore
process is designed to act independently on each table from the backup set,
rather than restoring the whole set one transaction at a time.

The resulting backup is continuous and incremental. Together with the the
initial SQL dump (also called basebackup) of the table, LBT writes ongoing
changes (called deltas). Once the number of deltas reaches the configured
threshold or the time since the previous basebackup exceeds a certain interval
provided in the configuration, the new basebackup is produced. Either of those
conditions leads to a new basebackup; all deltas recorded before the start of
the latest full dump are purged once the dump succeeds.

## Requirements

The Logical Backup Tool fetches the stream of changes using the PostgreSQL
feature called `logical decoding` and groups tables to back up with the concept
of a `publication`, an entity introduced in PostgreSQL 10 to describe a set of
tables that are replicated together; therefore, all PostgreSQL versions starting
from 10 are supported. You will need a direct connection to your database with
the user that either has login and replication permissions, given that the
publication specified in the tool configuration already exists, or the superuser
permissions if you want the tool to create it. You should have enough space on
the server running LBT to store at least 2x the size of N biggest tables used in
the publication, where N is the number of concurrent base backup processes from
the configuration; in a real-world scenario the actual space requirement depends
on a backupThreshold, i.e., the number of deltas accumulated between consecutive
full backups. On a busy system doing a lot of data changes with a
backupThreshold set too high it may require a magnitude of the original table
size; on a side note, such systems probably won't fit the typical OLAP use-case.

The tool normal operations (particularly how often the dumps are created) would
be disrupted if system clock is adjusted, however, switching from/to DST should
not lead to any issues.

## How does it work

On the initial run, the tool connects to the target database, makes a
publication when necessary and calls `pg_create_logical_replication_slot` to
create a new replication slot for the built-in output plugin called `pgoutput`.
It establishes the replication connection to the database and starts streaming
data from the slot created in the previous step. Once the changes for the not
yet observed table are received from the logical stream, the logical backup
routine puts to the workers queue a request to create the basebackup for that
table and goes on with writing a delta corresponding to that change on disk. One
of the spare backup workers (defined by the `concurrentBaseBackups`
configuration parameter) will eventually pick a request for the basebackup and
produce a `COPY targettable to STDOUT` dump of the table.

When LBT resumes after a period of downtime, it continues streaming from the
previously created slot; the slot provides a guarantee that the changes that the
tool haven't processed are not lost during the period when they are not
consumed. On the downside, the unconsumed changes will accumulate on the
database server, taking up disk space in the `wal` directory; therefore, when
planning a prolonged downtime of the backup tool one should drop the slot used
for the backup with a `pg_drop_replication_slot()` command. After the slot is
dropped, the backup directory should be purged, resulting in the backup process
to start from scratch; alternatively, set the `initialBasebackup` described
below. 
 
## Configuration parameters

LBT reads its configuration from the YAML file supplied as a command-line
argument. The following keys can be defined in that file:

* **tempDir**
  The directory to store temp files, such as incomplete basebackups.
  Once completed, those files will be moved to the main backup directory.
  
* **deltasPerFile** 
  The maximum amount of individual changes (called deltas) a
  single delta file may contain. This is the hard limit; once reached, LBT
  writes all subsequent deltas to the new file, even if that results in a single
  transaction to be split between multiple delta files.

* **backupThreshold**
  If the tool writes more than `backupThreshold` delta files
  since the last basebackup, the new basebackup for the table is requested.
  Setting this value too low will result in too many basebackups, setting it too
  high may produce too many changes, consuming more disk space than necessary
  and resulting in the longer recovery time for the table.
   
* **concurrentBasebackups**
  The maximum number of processes doing basebackups
  that can operate concurrently. Each process consumes a single PostgreSQL
  connection and runs COPY for a table it is tasked with, writing the outcome
  into a file.
   
* **trackNewTables**
   When set to true, allow starting the tool with an empty
   publication and permit new tables to be added to the initial set provided by
   the publication. It's a good idea to enable this option if you define a
   publication `FOR ALL TABLES` or make the LBT define the one for you.
   
* **slotname**
  Name of the logical replication slot that the tool should use.
  LBT attempts to create the slot if it doesn't exist. It expects a
  non-temporary slot with the output plugin `pgoutput`. Note that LBT never
  drops the slot on its own, if you need to start from scratch, you should drop
  it manually with `pg_drop_replication_slot`
   
* **publication**
  The name of the publication LBT should use to determine the
  set of tables to backup. LBT attempts to create one if it doesn't exist,
  defining it `FOR ALL TABLES`. If you need only a subset of tables you should
  create the corresponding publication beforehand.
    
* **sendStatusOnCommit**
  Determines whether to send the standby status message
  to the server on every commit. The server will act on a status message by
  adjusting the WAL position of the data that is confirmed to be written by the
  client, resulting in faster recycling of the old segments; however, in the
  case of multiple small transactions sending the status after recording the
  commit results in significant communication overhead. Given that the client
  sends those status message every 10 seconds, we don't recommend enabling this
  option unless you know exactly what you are doing.
    
* **initialBasebackup** 
  If set to true, LBT will trigger the initial basebackup
  for all tables in the publication at startup. This feature could be useful to
  discard the old backup deltas and start from the fresh basebackup, i.e., after
  deleting the replication slot.
    
* **fsync**
  When set to true, runs fsync, causing each write to the delta file
  to be durable. On a system with many writes this may negatively impact the
  performance.

* **archiveDir**
  Main directory to store the resulting backup.
     
* **periodBetweenBackups**
  Unconditionally force the new basebackup if the last one is older than the
  period specified in this parameter.
    
 * **oldDeltaBackupTrigger**
 Maximum time of no actviity on a table to trigger
 the new basebackup. Usually, on a table without any activity, the old deltas
 are retained for the whole `periodBetweenBackups`, which could be rather
 long. If there is a data retention policy that mandates to keep the old data
 for not more than a certain amount of time, it makes sense to set the
 `oldDeltaBackupTrigger` to a value slightly less than the retention interval
 to make sure the old data is removed from the backups even if nothing happens
 on the table.
 
* **db**
  Database connection parameters. The following values are accepted.
  * **host**:
  database server hostname or ip addresses
  * **port**:
  the port the database server listens to
  * **user**:
  the user name for the connection. See the `Requirements` part for the privilege
  this user must have.
  * **database**:  
  the database to connnect to. It is not possible to backup multiple databases
  with one insance of the tool at the moment; however, multiple backup tools can
  work on the same cluster on different databases.

All interval parameters (`periodBetweenBackups` and `oldDeltaBackupTrigger`)
values should have an integer with the time unit attached; valid units are 's',
'm', 'h' for seconds, minutes and hours. For instance, the value of `10h5s`
correspoonds to `10 hours 5 seconds`.
