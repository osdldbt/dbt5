#!/bin/sh

#
# This file is released under the terms of the Artistic License.
# Please see the file LICENSE, included in this package, for details.
#
# Copyright (C) 2004 Mark Wong & Open Source Development Lab, Inc.
#
# 2006 Rilson Nascimento

if [ $# -ne 1 ]; then
	echo "usage: gen_html.sh <results directory>"
fi

DIR=$1

echo '<html>' > ${DIR}/index.html

echo '<head>' >> ${DIR}/index.html
echo '<title>DBT-5 PostgreSQL Results</title>' >> ${DIR}/index.html
echo '</head>' >> ${DIR}/index.html

echo '<body>' >> ${DIR}/index.html

echo '<pre>' >> ${DIR}/index.html
cat ${DIR}/readme.txt >> ${DIR}/index.html
echo '</pre>' >> ${DIR}/index.html
echo '<hr/>' >> ${DIR}/index.html

echo '<pre>' >> ${DIR}/index.html
cat ${DIR}/driver/results.out >> ${DIR}/index.html
echo '</pre>' >> ${DIR}/index.html

echo '<hr/>' >> ${DIR}/index.html

echo 'System Statistics<br/>' >> ${DIR}/index.html
echo '<a href="iostatx.out">iostat</a><br/>' >> ${DIR}/index.html
echo '<a href="vmstat.out">vmstat</a> [<a href="vmcharts.html">charts</a>]<br/>' >> ${DIR}/index.html
echo '<a href="sar.out">sar</a><br/>' >> ${DIR}/index.html

echo '<hr/>' >> ${DIR}/index.html

echo 'Kernel and Application Profiles<br/>' >> ${DIR}/index.html
echo '<a href="readprofile_ticks.txt">readprofile</a><br/>' >> ${DIR}/index.html
echo '<a href="oprofile.txt">oprofile</a><br/>' >> ${DIR}/index.html
echo '<a href="callgraph.txt">callgraph</a><br/>' >> ${DIR}/index.html
#echo '<a href="oprofile/current">oprofile raw data</a><br/>' >> ${DIR}/index.html
echo '<a href="oprofile/annotate">opannotate source output</a><br/>' >> ${DIR}/index.html
echo '<a href="oprofile/assembly.txt">opannotate assembly output</a><br/>' >> ${DIR}/index.html

echo '<hr/>' >> ${DIR}/index.html

echo 'Operating System Information<br/>' >> ${DIR}/index.html
echo '<a href="proc.out">linux /proc</a><br/>' >> ${DIR}/index.html

echo '<hr/>' >> ${DIR}/index.html

echo 'Database Information<br/>' >> ${DIR}/index.html
echo '<a href="db/param.out">database parameters</a><br/>' >> ${DIR}/index.html
echo '<a href="db/plan0.out">explain plans</a><br/>' >> ${DIR}/index.html

echo '<hr/>' >> ${DIR}/index.html

echo 'Test Output Files<br/>' >> ${DIR}/index.html
echo '<a href="client">client output</a><br/>' >> ${DIR}/index.html
echo '<a href="driver">driver output</a><br/>' >> ${DIR}/index.html
echo '<a href="db">database output</a><br/>' >> ${DIR}/index.html

echo '<hr/>' >> ${DIR}/index.html

echo 'Trade-Result Transaction per Second<br/>' >> ${DIR}/index.html
echo '<img src="./driver/trtps.png" />' >> ${DIR}/index.html

echo '<hr/>' >> ${DIR}/index.html

echo 'Processor Utilization<br/>' >> ${DIR}/index.html
echo '<img src="plots/cpu.png" />' >> ${DIR}/index.html

echo '<hr/>' >> ${DIR}/index.html

echo '<a href="db.html">Database Charts</a><br/>' >> ${DIR}/index.html

# Database Charts
echo '<html>' > ${DIR}/db.html
echo '<head>' >> ${DIR}/db.html
echo '<title>DBT-5 PostgreSQL Database Charts</title>' >> ${DIR}/db.html
echo '</head>' >> ${DIR}/db.html
echo '<body>' >> ${DIR}/db.html
echo 'Index Scans<br/>' >> ${DIR}/db.html
echo '<img src="./db/indexes_scan.png" />' >> ${DIR}/db.html

echo '<hr/>' >> ${DIR}/db.html

echo 'Index Blocks Read<br/>' >> ${DIR}/db.html
echo '<img src="./db/index_info.png" />' >> ${DIR}/db.html

echo '<hr/>' >> ${DIR}/db.html

echo 'Table Blocks Read<br/>' >> ${DIR}/db.html
echo '<img src="./db/table_info.png" />' >> ${DIR}/db.html
echo '</body>' >> ${DIR}/db.html
echo '</html>' >> ${DIR}/db.html

echo '<hr/>' >> ${DIR}/index.html

echo '<a href="rt.html">Response Time Charts</a><br/>' >> ${DIR}/index.html

#
# Response Time Charts
#
echo '<html>' > ${DIR}/rt.html
echo '<head>' >> ${DIR}/rt.html
echo '<title>DBT-5 PostgreSQL Response Time Charts</title>' >> ${DIR}/rt.html
echo '</head>' >> ${DIR}/rt.html
echo '<body>' >> ${DIR}/rt.html

echo 'Broker-Volume Response Time Distribution<br/>' >> ${DIR}/rt.html
echo '<img src="driver/broker_volume.png" />' >> ${DIR}/rt.html

echo '<hr/>' >> ${DIR}/rt.html

echo 'Customer-Position Response Time Distribution<br/>' >> ${DIR}/rt.html
echo '<img src="driver/customer_position.png" />' >> ${DIR}/rt.html

echo '<hr/>' >> ${DIR}/rt.html

echo 'Data-Maintenance Response Time Distribution<br/>' >> ${DIR}/rt.html
echo '<img src="driver/data_maintenance.png" />' >> ${DIR}/rt.html

echo '<hr/>' >> ${DIR}/rt.html

echo 'Market-Feed Time Distribution<br/>' >> ${DIR}/rt.html
echo '<img src="driver/market_feed.png" />' >> ${DIR}/rt.html

echo '<hr/>' >> ${DIR}/rt.html

echo 'Market-Watch Response Time Distribution<br/>' >> ${DIR}/rt.html
echo '<img src="driver/market_watch.png" />' >> ${DIR}/rt.html

echo '<hr/>' >> ${DIR}/rt.html

echo 'Security-Detail Response Time Distribution<br/>' >> ${DIR}/rt.html
echo '<img src="driver/security_detail.png" />' >> ${DIR}/rt.html

echo '<hr/>' >> ${DIR}/rt.html

echo 'Trade-Lookup Response Time Distribution<br/>' >> ${DIR}/rt.html
echo '<img src="driver/trade_lookup.png" />' >> ${DIR}/rt.html

echo '<hr/>' >> ${DIR}/rt.html

echo 'Trade-Order Response Time Distribution<br/>' >> ${DIR}/rt.html
echo '<img src="driver/trade_order.png" />' >> ${DIR}/rt.html

echo '<hr/>' >> ${DIR}/rt.html

echo 'Trade-Result Response Time Distribution<br/>' >> ${DIR}/rt.html
echo '<img src="driver/trade_result.png" />' >> ${DIR}/rt.html

echo '<hr/>' >> ${DIR}/rt.html

echo 'Trade-Status Response Time Distribution<br/>' >> ${DIR}/rt.html
echo '<img src="driver/trade_status.png" />' >> ${DIR}/rt.html

echo '<hr/>' >> ${DIR}/rt.html

echo 'Trade-Update Response Time Distribution<br/>' >> ${DIR}/rt.html
echo '<img src="driver/trade_update.png" />' >> ${DIR}/rt.html

echo '<hr/>' >> ${DIR}/rt.html

# Response Time plots
#
echo 'Broker-Volume Response Time<br/>' >> ${DIR}/rt.html
echo '<img src="driver/bv_tran_bar.png" />' >> ${DIR}/rt.html

echo '<hr/>' >> ${DIR}/rt.html

echo 'Customer-Position Response Time<br/>' >> ${DIR}/rt.html
echo '<img src="driver/cp_tran_bar.png" />' >> ${DIR}/rt.html

echo '<hr/>' >> ${DIR}/rt.html

echo 'Data-Maintenance Response Time<br/>' >> ${DIR}/rt.html
echo '<img src="driver/dm_tran_bar.png" />' >> ${DIR}/rt.html

echo '<hr/>' >> ${DIR}/rt.html

echo 'Market-Feed Response Time<br/>' >> ${DIR}/rt.html
echo '<img src="driver/mf_tran_bar.png" />' >> ${DIR}/rt.html

echo '<hr/>' >> ${DIR}/rt.html

echo 'Market-Watch Response Time<br/>' >> ${DIR}/rt.html
echo '<img src="driver/mw_tran_bar.png" />' >> ${DIR}/rt.html

echo '<hr/>' >> ${DIR}/rt.html

echo 'Security-Detail Response Time<br/>' >> ${DIR}/rt.html
echo '<img src="driver/sd_tran_bar.png" />' >> ${DIR}/rt.html

echo '<hr/>' >> ${DIR}/rt.html

echo 'Trade-Lookup Response Time<br/>' >> ${DIR}/rt.html
echo '<img src="driver/tl_tran_bar.png" />' >> ${DIR}/rt.html

echo '<hr/>' >> ${DIR}/rt.html

echo 'Trade-Order Response Time<br/>' >> ${DIR}/rt.html
echo '<img src="driver/to_tran_bar.png" />' >> ${DIR}/rt.html

echo '<hr/>' >> ${DIR}/rt.html

echo 'Trade-Result Response Time<br/>' >> ${DIR}/rt.html
echo '<img src="driver/tr_tran_bar.png" />' >> ${DIR}/rt.html

echo '<hr/>' >> ${DIR}/rt.html

echo 'Trade-Status Response Time<br/>' >> ${DIR}/rt.html
echo '<img src="driver/ts_tran_bar.png" />' >> ${DIR}/rt.html

echo '<hr/>' >> ${DIR}/rt.html

echo 'Trade-Update Response Time<br/>' >> ${DIR}/rt.html
echo '<img src="driver/tu_tran_bar.png" />' >> ${DIR}/rt.html

echo '</body>' >> ${DIR}/rt.html
echo '</html>' >> ${DIR}/rt.html

echo '<hr/>' >> ${DIR}/index.html
echo '</body>' >> ${DIR}/index.html
echo '</html>' >> ${DIR}/index.html

# vmstat Charts

echo "<html>" > ${DIR}/vmcharts.html
echo "<head>" >> ${DIR}/vmcharts.html
echo "<title>DBT-5 Result vmstat Charts</title>" >> ${DIR}/vmcharts.html
echo "</head>" >> ${DIR}/vmcharts.html
echo "<body>" >> ${DIR}/vmcharts.html
echo "<img src=\"plots/cpu.png\" /><br />" >> ${DIR}/vmcharts.html
echo "<hr />" >> ${DIR}/vmcharts.html
echo "<img src=\"plots/cs.png\" /><br />" >> ${DIR}/vmcharts.html
echo "<hr />" >> ${DIR}/vmcharts.html
echo "<img src=\"plots/in.png\" /><br />" >> ${DIR}/vmcharts.html
echo "<hr />" >> ${DIR}/vmcharts.html
echo "<img src=\"plots/io.png\" /><br />" >> ${DIR}/vmcharts.html
echo "<hr />" >> ${DIR}/vmcharts.html
echo "<img src=\"plots/memory.png\" /><br />" >> ${DIR}/vmcharts.html
echo "<hr />" >> ${DIR}/vmcharts.html
echo "<img src=\"plots/procs.png\" /><br />" >> ${DIR}/vmcharts.html
echo "<hr />" >> ${DIR}/vmcharts.html
echo "<img src=\"plots/swap.png\" /><br />" >> ${DIR}/vmcharts.html
echo "</body>" >> ${DIR}/vmcharts.html
echo "</html>" >> ${DIR}/vmcharts.html
