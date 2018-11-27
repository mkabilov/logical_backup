#!/usr/bin/env bash

function finish {
  if [[ ${BACKUP_PID} -ne 0 ]]; then
    kill ${BACKUP_PID}
  fi
}

  rm -rf /tmp/backup /tmp/restore /tmp/final
}

rm -rf /tmp/final

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

pgbench lb_test_src --no-vacuum --file pgbench.sql --time 20 --jobs 40 --client 40 &> /dev/null
#pgbench lb_test1 --no-vacuum --file pgbench.sql --time 2 --jobs 3 --client 2 &> /dev/null
#pgbench lb_test1 --no-vacuum --file pgbench.sql --time 2 --jobs 2 --client 2 &> /dev/null

sleep 10

kill ${BACKUP_PID}
if [[ $? -ne 0 ]]; then
        echo "failed to kill backup process"
        exit 1
fi
BACKUP_PID=0

sleep 1

echo "-------------- Restore  --------------"

/tmp/restore -db lb_test_dst -backup-dir /tmp/final -host localhost -user $USER -table test
/tmp/restore -db lb_test_dst -backup-dir /tmp/final -host localhost -user $USER -table test2

echo "-------------------------------------------"

HASH1=$(psql -At -d lb_test_src -c "select md5(array_agg(t order by t)::text) from test t;")
HASH2=$(psql -At -d lb_test_dst -c "select md5(array_agg(t order by t)::text) from test t;")

if [[ "$HASH1" == "$HASH2" ]]; then
    echo "----------- test tables are equal ---------"
else
    psql -At -d lb_test_src -c "copy (select * from test order by id) to stdout" > /tmp/final/test_src
    psql -At -d lb_test_dst -c "copy (select * from test order by id) to stdout" > /tmp/final/test_dst
    echo "-------- test tables are different --------"
fi
echo "-------------------------------------------"


HASH1=$(psql -At -d lb_test_src -c "select md5(array_agg(t order by t)::text) from test2 t;")
HASH2=$(psql -At -d lb_test_dst -c "select md5(array_agg(t order by t)::text) from test2 t;")

if [[ "$HASH1" == "$HASH2" ]]; then
    echo "----------- test2 tables are equal --------"
else
    psql -At -d lb_test_src -c "copy (select * from test2 order by id) to stdout" > /tmp/final/test2_src
    psql -At -d lb_test_dst -c "copy (select * from test2 order by id) to stdout" > /tmp/final/test2_dst
    echo "-------- test2 tables are different -------"
fi
echo "-------------------------------------------"