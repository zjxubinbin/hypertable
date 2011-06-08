#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SCRIPT_DIR=`dirname $0`
NUM_POLLS=${NUM_POLLS:-"10"}
WRITE_SIZE=${WRITE_SIZE:-"40000000"}
RS1_PIDFILE=$HT_HOME/run/Hypertable.RangeServer.rs1.pid
RS2_PIDFILE=$HT_HOME/run/Hypertable.RangeServer.rs2.pid

$HT_HOME/bin/start-test-servers.sh --clear --no-rangeserver

$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$RS1_PIDFILE \
   --Hypertable.RangeServer.ProxyName=rs1 \
   --Hypertable.RangeServer.Port=38060 \
   --Hypertable.RangeServer.Maintenance.Interval 100 \
   --Hypertable.RangeServer.Range.SplitSize=250K 2>1 > rangeserver.rs1.output&

$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$RS2_PIDFILE \
   --Hypertable.RangeServer.ProxyName=rs2 \
   --Hypertable.RangeServer.Port=38061 \
   --Hypertable.RangeServer.Maintenance.Interval 100 \
   --Hypertable.RangeServer.Range.SplitSize=250K 2>1 > rangeserver.rs2.output&

$HT_HOME/bin/ht shell --no-prompt < $SCRIPT_DIR/create-table.hql

$HT_HOME/bin/ht ht_load_generator update \
    --Hypertable.Mutator.FlushDelay=50 \
    --rowkey.component.0.type=integer \
    --rowkey.component.0.format="%010lld" \
    --rowkey.component.0.min=0 \
    --rowkey.component.0.max=10000 \
    --Field.value.size=1000 \
    --max-bytes=$WRITE_SIZE

sleep 3

echo "shutdown; quit;" | $HT_HOME/bin/ht rsclient localhost:38061
echo "shutdown; quit;" | $HT_HOME/bin/ht rsclient localhost:38060

sleep 1

kill -9 `cat $HT_HOME/run/Hypertable.RangeServer.rs?.pid`

sleep 1

$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$RS1_PIDFILE \
   --Hypertable.RangeServer.ProxyName=rs1 \
   --Hypertable.RangeServer.Port=38062 2>1 >> rangeserver.rs1.output&

$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$RS2_PIDFILE \
   --Hypertable.RangeServer.ProxyName=rs2 \
   --Hypertable.RangeServer.Port=38063 2>1 >> rangeserver.rs2.output&

sleep 3

echo "About to load data ... " + `date`

#
# Load Test
#

$HT_HOME/bin/ht ht_load_generator update \
    --Hypertable.Mutator.FlushDelay=50 \
    --rowkey.component.0.type=integer \
    --rowkey.component.0.format="%010lld" \
    --rowkey.component.0.min=0 \
    --rowkey.component.0.max=10000 \
    --Field.value.size=1000 \
    --max-bytes=100000000 2>1 > load.output&

for ((i=0; i<20; i++)) ; do
  $SCRIPT_DIR/generate_range_move.py | $HT_HOME/bin/ht shell --batch
done

kill -9 `cat $HT_HOME/run/Hypertable.RangeServer.rs?.pid`

#$HT_HOME/bin/ht shell --batch < $SCRIPT_DIR/dump-table.hql > keys.output

#diff keys.output ${SCRIPT_DIR}/keys.golden
