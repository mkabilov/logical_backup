#!/usr/bin/env bash

function finish {
  if [[ ${BACKUP_PID} -ne 0 ]]; then
    kill ${BACKUP_PID}
  fi

  psql -d postgres -c "drop database if exists lb_test1;"
  psql -d postgres -c "drop database if exists lb_test2;"
}

rm -rf /tmp/backup /tmp/restore /tmp/final

psql -c "create role postgres superuser login" -d postgres

psql -f init.sql -v ON_ERROR_STOP=1 -d postgres

go build -o /tmp/backup ../cmd/backup
if [[ $? -ne 0 ]]; then
        echo "failed to build backup tool"
        exit 1
fi

go build -o /tmp/restore ../cmd/restore
if [[ $? -ne 0 ]]; then
        echo "failed to build restore tool"
        exit 1
fi

echo "-------------- Backup  --------------"

/tmp/backup ./config.yaml &
BACKUP_PID=$!

trap finish EXIT

pgbench lb_test1 --no-vacuum --file pgbench.sql --time 5 --jobs 5 --client 2

kill ${BACKUP_PID}
if [[ $? -ne 0 ]]; then
        echo "failed to kill backup process"
        exit 1
fi
BACKUP_PID=0

sleep 1

echo "-------------- Restore  --------------"

/tmp/restore -db lb_test2 -backup-dir /tmp/final -host localhost -user $USER -table test

echo "--------------------------------------"

HASH1=$(psql -At -d lb_test1 -c "select md5(array_agg(t order by t)::text) from test t;")
HASH2=$(psql -At -d lb_test2 -c "select md5(array_agg(t order by t)::text) from test t;")

if [[ "$HASH1" == "$HASH2" ]]; then
    echo "----------- tables are equal ---------"
else
    echo "-------- tables are different --------"
fi
echo "--------------------------------------"
