<?php
$rowperloop = 50000;
$time=1;
while($time > 0.1){
	$start = microtime(true);
	echo exec('php events_siphon.php 0 ' . $rowperloop) . "\n";
	$end = microtime(true);
	$time = $end - $start;
	$rps = $rowperloop / $time;
	echo $rowperloop . ' rows processed in ' . number_format($time, 4) . ' seconds, which is ' . number_format($rps) . ' per second.' . "\n" ;
	sleep(1);
}