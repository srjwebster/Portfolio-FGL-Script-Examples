<?php

set_include_path(__DIR__);
ini_set('display_errors', 'on');
error_reporting(E_ALL);

$configJson = file_get_contents(dirname(__DIR__) . '/x.json');
if (!$configJson) throw new Exception("Could not load the x.json file");

$config = json_decode($configJson, true);

$mysqlRemote = new MySQLi($config['remoteDatabase']['host'],
    $config['remoteDatabase']['username'],
    $config['remoteDatabase']['password'],
    $config['remoteDatabase']['database']);

$mysqlLocal = new MySQLi($config['localDatabase']['host'],
    $config['localDatabase']['username'],
    $config['localDatabase']['password'],
    $config['localDatabase']['database']);

$isStaging = ($config['localDatabase']['database'] == 'analytics_staging');
$minIndex = 0;
$res = $mysqlLocal->query('SELECT max(`highest_id`) FROM x');
$row = $res->fetch_array();
$minIndex = intval($row[0]);

$mysqlRemote->query('SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED');

$batchSize = $config['batchSize'];
$finished = false;
$index = 0;

$minEventId = PHP_INT_MAX;
$maxEventId = -1;
$eventCount = 0;
$skippedEvents = 0;

$excludedPackages = $config['excludedPackages'];
$importantPackages = $config['importantPackages'];
$eventsColumns = "(`x`, `x`, `x`, `x`, `x`, `x`, `x`, `x`, `x`, `x`, `x`, `x`, `x`, `x`, `x`)";
$sessionStartColumns = "(`x`, `x`,`x`, `x`, `x`, `x`)";
$sessionUpdateColumns = "(`x`, `x`, `x`, `x`, `x`, ``, `x`, `x`, `x`, `x`)";
$webhooks = [];
$bootEvents = [];
while (!$finished) {
    $iterator = $mysqlRemote->query('SELECT *, inet_ntoa(ip) AS ip_addr FROM x LIMIT ' . $batchSize);
    $batchValues = [];
    $referrerValues = [];
    $bootValues = [];
    $installValues = [];
    $rewardValues = [];
    $impressionValues = [];
    $enhancedAppValues = [];
    $importantPackagesValues = [];
    $deleteEventIds = [];
    $ipStorage = [];
    $sessionStart = [];
    $sessionsData = [];

    while ($row = $iterator->fetch_assoc()) {

        $id = $row['event_id'];
        if ($id < $minEventId) $minEventId = $id;
        if ($id > $maxEventId) $maxEventId = $id;
        $deleteEventIds[] = $row['x'];
        if (in_array($row['x'], $excludedPackages)) {
            $skippedEvents++;
            continue;
        }

        if ($row['x'] == 'x' || $row['x'] == 'x') {
            $sessionStart[] = '(' . implode(',', [
                    $row['x'],
                    '"' . $mysqlLocal->real_escape_string($row['x']) . '"',
                    '"' . $mysqlLocal->real_escape_string($row['x']) . '"',
                    '"' . $mysqlLocal->real_escape_string($row['x']) . '"',
                    '"' . $mysqlLocal->real_escape_string($row['x']) . '"',
                    is_null($row['x']) ? 'NULL' : $row['x']
                ]) . ')';
        }


        if (!empty($row['x'])) {
            if (!isset($sessionsData[$row['x']])) {
                $sessionsData[$row['x']] = [
                    'x' => strtotime($row['timestamp']),
                    'x' => 0,
                    'x' => 0,
                    'x' => 0,
                    'x' => 0,
                    'x' => 0,
                    'x' => 0,
                    'x' => 0,
                    'x' => 0,
                    'x' => 0
                ];
            }
            $rowTime = strtotime($row['timestamp']);
            if ($rowTime > $sessionsData[$row['x']]['x']) {
                $sessionsData[$row['x']]['x'] = $rowTime;
            }
            if ($row['x'] == 'ad_display_success' ||( $row['x'] == 'x' && $row['x'] == 'x' ) || ( $row['x'] == 'x' && $row['x'] == 'x')) {
                $sessionsData[$row['x']]['x']++;
            } elseif (($row['x'] == 'x' && $row['x'] == 'x' )|| $row['x'] == 'x') {
                $sessionsData[$row['x']]['x']++;
            } elseif ($row['x'] == 'x' || $row['x'] == 'x' || $row['x'] == 'x' || $row['x'] == 'x') {
                $sessionsData[$row['x']]['x']++;
            } elseif ($row['x'] == 'x' && $row['x'] == 'x') {
                $sessionsData[$row['x']]['x']++;
            } elseif (($row['x'] == 'x' && $row['x'] == '' && $row['x'] == 'x' )|| ($row['x'] == 'x' && $row['x'] == 'x')) {
                $sessionsData[$row['x']]['x']++;
            } elseif (($row['x'] == 'x' && $row['x'] == 'x' )|| ($row['x'] == 'x' && $row['x'] == 'x' )|| ($row['x'] == 'x' && $row['x'] == 'x')) {
                $sessionsData[$row['x']]['x']++;
            }
            // add in_app and video_theatre here, once they're/if they're ever needed.
        }

        if ($row['x'] == 'x' || $row['x'] == 'x') {
            $bootEvents[] = $row['x'] . ',' . $row['x'];
        }

        //$webhooks['lpm.' . $row['x']] = '' . urlencode(substr($row['x'], 3)) . '';

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
                '"' . $mysqlLocal->real_escape_string($row['x']) . '"',
                '"' . $mysqlLocal->real_escape_string($row['x']) . '"'
            ]) . ')';
        $eventCount++;
        $batchValues[] = $eventsValues;

        $ipStorage[] = '(' . implode(', ', [
                is_null($row['x']) ? 'NULL' : $row['x'],
                '"' . $mysqlLocal->real_escape_string($row['x']) . '"',
            ]) . ')';
        if ($row['x'] == '.' && $row['x'] == 'x' && $row['x'] == 'x') {
            $webhooks['x' . $row['x']] = '' . $row['x'];
        }
        if ($row['x'] == '' && $row['x'] == 'x' && $row['x'] == 'x') {
            $webhooks['x' . $row['x']] = '' . $row['x'] . '';
        }
        if (($row['x'] == 'referrer' || $row['x'] == 'x') && !empty($row['x']) && $row['x'] != 'x-x' && $row['x'] != '(not-set)') {
            $referrerValues[] = $eventsValues;
            if ($row['x'] == '' && $row['x'] == 'x') {
                $webhooks['x' . $row['x']] = '' . $row['x'];
            }
            if ($row['x'] == '' && $row['x'] == 'x') {
                $webhooks['imou2_' . $row['x']] = '' . urlencode($row['x']) . '';
            }
            if ($row['x'] == '' && $row['x'] == 'x') {
                $webhooks['imtm3_b_' . $row['x']] = '' . urlencode($row['x']) . '';
                $webhooks['imtm3_i_' . $row['x']] = '' . urlencode($row['x']) . '';
            }
        } elseif ($row['x'] == 'x' || $row['x'] == 'x') {
            $bootValues[] = $eventsValues;
        } elseif ($row['x'] == 'x' || $row['x'] == 'x') {
            $installValues[] = $eventsValues;
        } elseif ($row['x'] == 'x' || $row['x'] == 'x' || $row['x'] == 'x' || $row['x'] == 'x') {
            $rewardValues[] = $eventsValues;
        } elseif ($row['x'] == 'x') {
            $impressionValues[] = $eventsValues;
        } elseif (substr($row['x'], 0, 2) == 'e_') {
            $enhancedAppValues[] = $eventsValues;
        } elseif (in_array($row['x'], $importantPackages)) {
            $importantPackagesValues[] = $eventsValues;
        }
    }

    if (count($batchValues) != 0) {
        $query = 'INSERT IGNORE INTO `x`' . $eventsColumns . ' VALUES ' . implode(',', $batchValues);
        if (!$mysqlLocal->query($query)) {
            throw new Exception($mysqlLocal->error);
        }
        echo "Stored " . $mysqlLocal->affected_rows . " rows [total " . $eventCount . "] into the x table\n";
    }
    if (count($sessionStart) != 0) {
        $query = 'INSERT IGNORE INTO `x` ' . $sessionStartColumns . ' VALUES ' . implode(', ', $sessionStart);
        if (!$mysqlLocal->query($query)) {
            throw new Exception($mysqlLocal->error);
        }
        echo "Stored " . $mysqlLocal->affected_rows . " rows [total " . count($sessionStart) . "] into the x table\n";

    }
    if (count($ipStorage) != 0) {
        $query = 'INSERT IGNORE INTO `x` (`ip`, `timestamp`) VALUES ' . implode(', ', $ipStorage);
        if (!$mysqlLocal->query($query)) {
            throw new Exception($mysqlLocal->error);
        }
        echo "Stored " . $mysqlLocal->affected_rows . " rows [total " . count($ipStorage) . "] into the x x table\n";
    }

    if (count($referrerValues) != 0) {
        $query = 'INSERT IGNORE INTO `x` ' . $eventsColumns . ' VALUES ' . implode(',', $referrerValues);
        if (!$mysqlLocal->query($query)) {
            throw new Exception($mysqlLocal->error);
        }
        echo "Stored " . $mysqlLocal->affected_rows . " rows [total " . count($referrerValues) . "] into the x table\n";
    }
    if (count($bootValues) != 0) {
        $query = 'INSERT IGNORE INTO `x` ' . $eventsColumns . ' VALUES ' . implode(',', $bootValues);
        if (!$mysqlLocal->query($query)) {
            throw new Exception($mysqlLocal->error);
        }
        echo "Stored " . $mysqlLocal->affected_rows . " rows [total " . count($bootValues) . "] into the x table\n";
    }
    if (count($installValues) != 0) {
        $query = 'INSERT IGNORE INTO `x` ' . $eventsColumns . ' VALUES ' . implode(',', $installValues);
        if (!$mysqlLocal->query($query)) {
            throw new Exception($mysqlLocal->error);
        }
        echo "Stored " . $mysqlLocal->affected_rows . " rows [total " . count($installValues) . "] into the x table\n";
    }
    if (count($rewardValues) != 0) {
        $query = 'INSERT IGNORE INTO `x` ' . $eventsColumns . ' VALUES ' . implode(',', $rewardValues);
        if (!$mysqlLocal->query($query)) {
            throw new Exception($mysqlLocal->error);
        }
        echo "Stored " . $mysqlLocal->affected_rows . " rows [total " . count($rewardValues) . "] into the x table\n";
    }
    if (count($impressionValues) != 0) {
        $query = 'INSERT IGNORE INTO `x` ' . $eventsColumns . ' VALUES ' . implode(',', $impressionValues);
        if (!$mysqlLocal->query($query)) {
            throw new Exception($mysqlLocal->error);
        }
        echo "Stored " . $mysqlLocal->affected_rows . " rows [total " . count($impressionValues) . "] into the x table\n";
    }

    if (count($enhancedAppValues) != 0) {
        $query = 'INSERT IGNORE INTO `x` ' . $eventsColumns . ' VALUES ' . implode(',', $enhancedAppValues);
        if (!$mysqlLocal->query($query)) {
            throw new Exception($mysqlLocal->error);
        }
        echo "Stored " . $mysqlLocal->affected_rows . " rows [total " . count($enhancedAppValues) . "] into the x table\n";
    }
    if (count($importantPackagesValues) != 0) {
        $query = 'INSERT IGNORE INTO `x` ' . $eventsColumns . ' VALUES ' . implode(',', $importantPackagesValues);
        if (!$mysqlLocal->query($query)) {
            throw new Exception($mysqlLocal->error);
        }
        echo "Stored " . $mysqlLocal->affected_rows . " rows [total " . count($importantPackagesValues) . "] into the x table\n";
    }

    $index++;
    $finished = $iterator->num_rows < $batchSize;
    if (count($deleteEventIds) != 0) {
        if (!$isStaging) {
            $deleteQuery = 'DELETE FROM x WHERE x IN (' . implode(',', $deleteEventIds) . ')';
            if (!$mysqlRemote->query($deleteQuery)) {
                throw new Exception($mysqlRemote->error);
            }
        } else {
            echo "Event IDs would have been deleted, but this is staging only.\n";
        }
    } else {
        echo "No events were captured, so there are none to delete.\n";
    }
    $inKeys = implode(',', array_keys($sessionsData));
    $results = $mysqlLocal->query("SELECT x, x FROM x WHERE session_id IN (" . $inKeys . ")");
    if (!$results) {
        echo "No events matched the stored sessions.\n";
    } else {
        while($matchedRow = $results->fetch_assoc()){
            $matchedSession = $sessionsData[$matchedRow['session_id']];
            if (strtotime($matchedRow['session_end']) > ($matchedSession['last_event_time'])){
                $matchedSession['last_event_time'] = strtotime($matchedRow['session_end']);
            }
            $query = "UPDATE x SET 
                x = '" . date('Y-m-d H:i:s', $matchedSession['last_event_time']) . "',
                x = x + " . $matchedSession['x'] . ",
                x = x + " . $matchedSession['x'] . ",
                x = x + " . $matchedSession['x'] . ",
                x = x + " . $matchedSession['x'] . ",
                x = x + " . $matchedSession['x'] . ",
                x = x + " . $matchedSession['x'] . ",
                x = x + " . $matchedSession['x'] . ",
                x = x + " . $matchedSession['x'] . ",
                x = x + " . $matchedSession['x'] . " WHERE x = " . $matchedRow['x'];
            if (!$mysqlLocal->query($query)) {
                throw new Exception($mysqlLocal->error);
            }
        }
    }
}

$query = 'INSERT INTO x (lowest_id, highest_id, event_count, skipped_count) VALUES (' . $minEventId . ',' . $maxEventId . ',' . $eventCount . ', ' . $skippedEvents . ')';
if (!$mysqlLocal->query($query)) {
    throw new Exception($mysqlLocal->error);
}

date_default_timezone_set('UTC');
$csvData = implode("\n", $bootEvents);
file_put_contents(dirname(__DIR__) . '/x-' . date('Y-m-d') . '.csv', $csvData . "\n", FILE_APPEND);

$context = stream_context_create(['http' => ['ignore_errors' => true]]);
foreach ($webhooks as $uniqueKey => $hook) {
    echo 'Sending webhook ' . $uniqueKey . "\n";
    $res = $mysqlLocal->query('SELECT `x` FROM `x` WHERE `x` = "' . $mysqlLocal->real_escape_string($uniqueKey) . '"');
    if ($res->num_rows > 0) continue;
    $sent = date('Y-m-d H:i:s');
    $start = microtime(true);
    $http_response_header = [];
    $response = @file_get_contents($hook, false, $context);
    $end = microtime(true);
    $status = isset($http_response_header[0]) ? $http_response_header[0] : 'unknown';
    $time = intval(($end - $start) * 1000);
    $query = 'INSERT INTO x (x, x, x, x, x, x) VALUES ('
        . '"' . $mysqlLocal->real_escape_string($hook) . '",'
        . '"' . $mysqlLocal->real_escape_string($uniqueKey) . '",'
        . '"' . $mysqlLocal->real_escape_string($sent) . '",'
        . $time . ','
        . '"' . $mysqlLocal->real_escape_string($status) . '",'
        . '"' . $mysqlLocal->real_escape_string($response) . '")';
    $mysqlLocal->query($query);
}
echo "Done!\n";
