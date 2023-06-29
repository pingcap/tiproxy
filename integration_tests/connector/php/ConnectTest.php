<?php
$host = $argv[1];
$username = $argv[2];
$password = $argv[3];
$database = 'test';
$port = 4000;
$ca_root_path = $argv[4];

$mysqli = mysqli_init();
$mysqli->ssl_set(NULL, NULL, $ca_root_path, NULL, NULL);
$mysqli->real_connect($host, $username, $password, $database, $port);

$result = $mysqli->query('SHOW DATABASES');
while ( $data = $result->fetch_array() ) {
      echo $data[0];
      echo "\n";
}
?>