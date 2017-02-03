<?php
$configJson = file_get_contents(dirname(__DIR__) . '/config.json');
if(!$configJson) throw new Exception("Could not load the config.json file");

$config = json_decode($configJson, true);
$mysqlLocal = new MySQLi($config['localDatabase']['host'],
    $config['localDatabase']['username'],
    $config['localDatabase']['password'],
    $config['localDatabase']['database']);
// selects the database on titan
$startIndex = 0;
// sets the start index as 0, as a backup if its not specified by the launch arguments
if(isset($argv[1])) $startIndex = intval($argv[1]);
// take the first argument and sets it as the start index
if($startIndex == 0){
    $startIndex = json_decode(file_get_contents('x.json'));
}
// if a start argument isn't otherwise specified, this will check the json file that stores the most recent row added to the table.
$count = 20000;
// sets the rows to complete in this query if not specified in an argument.
if(isset($argv[2])) $count = intval($argv[2]);
// sets the rows to complete if set in the argument
$results = $mysqlLocal->query("SELECT * FROM x WHERE x > " . $startIndex . " LIMIT " . $count);
// this is the query to retrieve each row in the events table starting at the previous event ID retreived for as many as we've specified in argv 2.

$maxid = $startIndex;
// sets a var maxid to the start argument or 0 if not set.
$importantPackages = $config['importantPackages'];
$referrerValues = [];
$bootValues = [];
$installValues = [];
$rewardValues = [];
$enhancedAppValues = [];
$importantPackagesValues =[];
// creates various arrays ready for inserting contents later.

while($row = $results->fetch_assoc()) {
// assigns the column names to the array
    $eventsValues = '(' . implode(',', [
        $row['x'],
        '"' . $mysqlLocal->real_escape_string($row['x']) . '"',
        '"' . $mysqlLocal->real_escape_string($row['x']) . '"',
        '"' . $mysqlLocal->real_escape_string($row['x']) . '"',
        is_null($row['x']) ? 'NULL' : $row['x'],
        is_null($row['x']) ? 'NULL' : $row['x'],
        '"' . $mysqlLocal->real_escape_string($row['x']) . '"',
        is_null($row['x']) ? 'NULL' : $row['x'],
        is_null($row['x']) ? 'NULL' : $row['x'],
        is_null($row['x']) ? 'NULL' : $row['x'],
        '"' . $mysqlLocal->real_escape_string($row['x']) . '"',
        '"' . $mysqlLocal->real_escape_string($row['x']) . '"',
        '"' . $mysqlLocal->real_escape_string($row['x']) . '"',
        '"' . $mysqlLocal->real_escape_string($row['x']) . '"'
    ]) . ')';
    // this is the storage array for the events tables rows

    // creates a variable to store the last event_id used
    if($row['x'] == 'x' && !empty($row['x']) && $row['x'] != 'x' && $row['x'] != '(not-set)'){
        $referrerValues[] = $eventsValues;
    }
    // creates array of referrer type events ready for insert
    elseif($row['x'] == 'x' || $row['x'] == 'x'){
        $bootValues[] = $eventsValues;
    }
    // creates array of boot type events ready for insert
    elseif($row['x'] == 'install' || $row['x'] == 'x'){
        $installValues[] = $eventsValues;
    }
    // creates array of install type events ready for insert
    elseif($row['x'] == 'x' || $row['x'] == 'x' || $row['x'] == 'x'){
        $rewardValues[] = $eventsValues;
    }
    elseif(substr($row['x'],0,2)== 'e_'){
        $enhancedAppValues[] = $eventsValues;
    }
    // If the event type id starts with e_
    elseif(in_array($row['x'], $importantPackages)){
        $importantPackagesValues[] = $eventsValues;
    }
    // If the package name is defined as 'important' in the config.json. Used for enhance web events currently.
    $maxid = intval($row['event_id']);
    // creates array of reward type events ready for insert
}

if(count($referrerValues) != 0) {
    $query = 'INSERT IGNORE INTO `x` VALUES ' . implode(',', $referrerValues);
    if (!$mysqlLocal->query($query)) {
        throw new Exception($mysqlLocal->error);
    }
    echo "Stored " . $mysqlLocal->affected_rows . " rows [total " . count($referrerValues) . "] into the x table\n";
}
if(count($bootValues) != 0) {
    $query = 'INSERT IGNORE INTO `x` VALUES ' . implode(',', $bootValues);
    if (!$mysqlLocal->query($query)) {
        throw new Exception($mysqlLocal->error);
    }
    echo "Stored " . $mysqlLocal->affected_rows . " rows [total " . count($bootValues) . "] into the x table\n";
}
if(count($installValues) != 0) {
    $query = 'INSERT IGNORE INTO `x` VALUES ' . implode(',', $installValues);
    if (!$mysqlLocal->query($query)) {
        throw new Exception($mysqlLocal->error);
    }
    echo "Stored " . $mysqlLocal->affected_rows . " rows [total " . count($installValues) . "] into the x table\n";
}
if(count($rewardValues) != 0) {
    $query = 'INSERT IGNORE INTO `x` VALUES ' . implode(',', $rewardValues);
    if (!$mysqlLocal->query($query)) {
        throw new Exception($mysqlLocal->error);
    }
    echo "Stored " . $mysqlLocal->affected_rows . " rows [total " . count($rewardValues) . "] into the x table\n";
}
if(count($enhancedAppValues) != 0) {
    $query = 'INSERT IGNORE INTO `x` VALUES ' . implode(',', $enhancedAppValues);
    if (!$mysqlLocal->query($query)) {
        throw new Exception($mysqlLocal->error);
    }
    echo "Stored " . $mysqlLocal->affected_rows . " rows [total " . count($enhancedAppValues) . "] into the x table\n";
}
else {echo count($enhancedAppValues) . " enhanced app events occurred since last sync.\n";}
if(count($importantPackagesValues) != 0) {
    $query = 'INSERT IGNORE INTO `x` VALUES ' . implode(',', $importantPackagesValues);
    if (!$mysqlLocal->query($query)) {
        throw new Exception($mysqlLocal->error);
    }
    echo "Stored " . $mysqlLocal->affected_rows . " rows [total " . count($importantPackagesValues) . "] into the x table\n";
}
else {echo count($importantPackagesValues) . " important package events occurred since last sync.\n";}


file_put_contents('x.json', json_encode($maxid));
// store progress
echo "Records written to events siphon tabled up to row " . $maxid . "\n" ;
//gives us an output.
echo "Done!\n";
