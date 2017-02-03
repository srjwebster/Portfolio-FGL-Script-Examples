<?php
$rowperloop = 500000;
while(true){
	$start = microtime(true);
	echo exec('php parse_events.php 0 ' . $rowperloop) . "\n";
	$end = microtime(true);
	$time = $end - $start;
	$rps = $rowperloop / $time;
	echo $rowperloop . ' rows processed in ' . number_format($time, 4) . ' seconds, which is ' . number_format($rps) . ' per second.' . "\n" ;
	sleep(1);
}