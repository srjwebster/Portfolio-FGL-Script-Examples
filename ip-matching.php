<?php

require_once dirname(__DIR__) . '/vendor/autoload.php';
use GeoIp2\Database\Reader;

$reader = new Reader(__DIR__ . '/GeoLite2-City.mmdb');
set_include_path(__DIR__);
ini_set('display_errors', 'on');
error_reporting(E_ALL);

$configJson = file_get_contents(dirname(__DIR__) . '/x.json');
if (!$configJson) throw new Exception("Could not load the x.json file");
$config = json_decode($configJson, true);

$batchSize = $config['batchSize'];

$mysqlLocal = new MySQLi($config['localDatabase']['host'],
    $config['localDatabase']['username'],
    $config['localDatabase']['password'],
    $config['localDatabase']['database']);

$query = 'SELECT * FROM `x` LIMIT' . $batchSize;
$queryResults = $mysqlLocal->query($query);
$deletionIps = [];
$batchValues = [];
while ($row = $queryResults->fetch_assoc()) {
    $ip = $row['ip'];
    $deletionIps[] = $ip;
    // add ip to an array for deletion after successful insert.
    if (empty($ip)) continue;

    try {
        $ipAddr = long2ip(intval($ip));
        $ipDetail = $reader->city($ipAddr);
    } catch (Exception $e) {
        echo "Could not decode: " . $ipAddr . "\n";
        continue;
    }
    $ipDetail = $reader->city($ipAddr);
    // set the ip address to get detail for.
    $ipAddr = '"' . $mysqlLocal->real_escape_string($ipAddr) . '"';
    $lat = !empty($ipDetail->location->latitude) ? $ipDetail->location->latitude : "NULL";
    $long = !empty($ipDetail->location->longitude) ? $ipDetail->location->longitude : "NULL";
    $country = !$ipDetail->country->isoCode ? "NULL" : '"' . $mysqlLocal->real_escape_string($ipDetail->country->isoCode) . '"';
    $state = !$ipDetail->mostSpecificSubdivision->name ? 'NULL' : '"' . $mysqlLocal->real_escape_string($ipDetail->mostSpecificSubdivision->name) . '"';
    $city = !$ipDetail->city->name ? 'NULL' : '"' . $mysqlLocal->real_escape_string($ipDetail->city->name) . '"';
    $time = '"' . $mysqlLocal->real_escape_string($row['timestamp']) . '"';
    $batchValues[] = '(' . implode(',', [
            $time,
            intval($ip),
            $ipAddr,
            $lat,
            $long,
            $country,
            $state,
            $city
        ]) . ')';
}


if (empty($batchValues)) {
    echo "No ips to sync.\n";
} else {
    $writeQuery = 'INSERT IGNORE INTO `x` (first_seen, ip, ip_addr, latitude, longitude, country, state, city) VALUES ' . implode(',', $batchValues);
    if (!$mysqlLocal->query($writeQuery)) {
        throw new Exception($mysqlLocal->error);
    }
    echo "Stored " . $mysqlLocal->affected_rows . " rows\n";
}
if (empty($deletionIps)) {
    echo "No ips to delete.\n";
} else {
    $deleteQuery = 'DELETE FROM `x` WHERE `ip` IN (' . implode(',', $deletionIps) . ')';
    if (!$mysqlLocal->query($deleteQuery)) {
        throw new Exception($mysqlLocal->error);
    }
    echo "Deleted " . $mysqlLocal->affected_rows . " rows\n";
}

echo "Done!\n";

