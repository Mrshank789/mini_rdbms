<?php

require_once 'Database.php';

$db = new Database();

echo "Mini RDBMS Shell (PHP)\nType 'exit' to quit.\n\n";

while (true) {
    $line = readline("db> ");
    
    if ($line === 'exit') break;
    if (empty(trim($line))) continue;

    // Add simple history support if readline is enabled
    if (function_exists('readline_add_history')) {
        readline_add_history($line);
    }

    $result = $db->query($line);
    echo $result . "\n";
}