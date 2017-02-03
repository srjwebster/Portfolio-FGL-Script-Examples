<?php
$start = microtime(true);
$configJson = file_get_contents(dirname(__DIR__) . '/config.json');
if (!$configJson) throw new Exception("Could not load the config.json file");

$config = json_decode($configJson, true);
$mysql = new MySQLi
(
    $config['localDatabase']['host'],
    $config['localDatabase']['username'],
    $config['localDatabase']['password'],
    $config['localDatabase']['database']
);
$files = [];
$releasedPackages = [];
$dir = scandir(dirname(__DIR__));
foreach ($dir as $file) { // We are only interested in CSV files
    if (strtolower(substr($file, -4)) != ".csv") continue;
    $files[] = $file;
}

foreach ($files as $file) {
    $date = substr($file, -14, 10);
    if ($date == date('Y-m-d')) {
        continue;
    }
    $csvData = fopen(dirname(__DIR__) . '/' . $file, 'r');
    $packageArray = [];
    while (!feof($csvData)) {
        $line = fgetcsv($csvData);
        $packageName = $line[0];
        if (isset($releasedPackages[$packageName])) {
            continue;
        }
        $ip = $line[1];
        if (isset($packageArray[$packageName])) {
            $packageArray[$packageName][] = $ip;
            if (count($packageArray[$packageName]) > 3) {
                $releasedPackages[$packageName] = $date;
            }
        } else {
            $packageArray[$packageName] = [$ip];
        }
    }
    fclose($csvData);
    $moveFile = "gzip -c " . dirname(__DIR__) . '/' . $file . " >" . dirname(__DIR__) . "/csvarchive/" . $file . ".gz";
    exec($moveFile);
    if (unlink(dirname(__DIR__) . '/' . $file)) {
        echo $file . " deleted successfully.\n";
    } else {
        echo $file . " NOT deleted successfully.\n";
    }
}
$values = [];
foreach ($releasedPackages as $packageName => $date) {
    $values[] = "('" . $mysql->real_escape_string($packageName) . "', '" . $mysql->real_escape_string($date) . "')";
}
if (count($values) == 0) {
    echo "No csv's to process.\n";
    exit;
}
$query = "INSERT IGNORE INTO `x` (package_name,release_date) VALUES" . implode(",", $values);
$mysql->query($query);
$affected = $mysql->affected_rows;
$end = microtime(true);
$time = $end - $start;
echo $affected . " records written to table in " . $time . " seconds.\n";

exit;
