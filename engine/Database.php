<?php
require_once 'Storage.php';

class Database
{
    private $storage;
    private $indexer;

    public function __construct() {
        $this->storage = new Storage();
        require_once 'Index.php';
        $this->indexer = new Index();
    }

    public function query($sql) {
        $sql = trim(preg_replace('/\s+/', ' ', $sql)); // Normalize whitespace

        try {
            if (stripos($sql, 'CREATE TABLE') === 0) return $this->createTable($sql);
            if (stripos($sql, 'INSERT INTO') === 0) return $this->insert($sql);
            if (stripos($sql, 'SELECT') === 0) return $this->select($sql);
            if (stripos($sql, 'UPDATE') === 0) return $this->update($sql);
            if (stripos($sql, 'DELETE') === 0) return $this->delete($sql);
            
            // Transaction Control
            if (strtoupper($sql) === 'BEGIN') return $this->storage->beginTransaction();
            if (strtoupper($sql) === 'COMMIT') return $this->storage->commit();
            if (strtoupper($sql) === 'ROLLBACK') return $this->storage->rollback();

            throw new Exception("Unknown command.");
        } catch (Exception $e) {
            return "Error: " . $e->getMessage();
        }
    }

    // --- 1. TABLE DEFINITION (Data Types & Constraints) ---
    private function createTable($sql) {
        // Regex: CREATE TABLE name (col type constraints, col type...)
        if (!preg_match('/CREATE TABLE (\w+) \((.*)\)/i', $sql, $matches)) {
            throw new Exception("Syntax Error in CREATE TABLE");
        }
        $tableName = $matches[1];
        $defsRaw = explode(',', $matches[2]);
        $schema = [];

        foreach ($defsRaw as $def) {
            $parts = explode(' ', trim($def));
            $colName = array_shift($parts);
            $colType = strtoupper(array_shift($parts));
            $constraints = implode(' ', $parts);

            $colDef = ['name' => $colName, 'type' => $colType];
            
            if (stripos($constraints, 'PRIMARY KEY') !== false) $colDef['pk'] = true;
            if (stripos($constraints, 'UNIQUE') !== false) $colDef['unique'] = true;

            $schema[$colName] = $colDef;
        }

        $this->storage->saveSchema($tableName, $schema);
        $this->storage->saveTableToDisk($tableName, []); // Init empty data

        // NEW: Create empty index files for PK/Unique
        foreach ($schema as $colName => $def) {
            if (isset($def['pk']) || isset($def['unique'])) {
                $this->indexer->rebuild($tableName, $colName, []);
            }
        }

        return "Table '$tableName' created.";
    }

    // --- 2. INSERT (Validation & Constraints) ---
    private function insert($sql) {
        // Format: INSERT INTO table VALUES (val1, val2)
        if (!preg_match('/INSERT INTO (\w+) VALUES \((.*)\)/i', $sql, $matches)) {
            throw new Exception("Syntax Error in INSERT");
        }
        $table = $matches[1];
        $values = array_map(function($v) {
            return trim($v, " '\"");
        }, explode(',', $matches[2]));

        $schema = $this->storage->loadSchema($table);
        $currentRows = $this->storage->selectAll($table);
        
        $newRow = [];
        $colNames = array_keys($schema);

        if (count($values) !== count($colNames)) {
            throw new Exception("Column count mismatch.");
        }

        // VALIDATION LOOP
        foreach ($colNames as $index => $colName) {
            $val = $values[$index];
            $def = $schema[$colName];

            // 1. Type Enforcement
            if ($def['type'] === 'INT' && !is_numeric($val)) throw new Exception("Column '$colName' must be INT");
            if ($def['type'] === 'BOOLEAN') $val = filter_var($val, FILTER_VALIDATE_BOOLEAN);

            // 2. Constraint Enforcement (PK & Unique)
            if (isset($def['pk']) || isset($def['unique'])) {
                foreach ($currentRows as $row) {
                    if ($row[$colName] == $val) {
                        throw new Exception("Constraint Violation: '$colName' must be unique. Value '$val' exists.");
                    }
                }
            }
            $newRow[$colName] = $val;
        }

        $currentRows[] = $newRow;
        $this->storage->saveRows($table, $currentRows);
        
        // NEW: Sync Index
        $this->updateIndex($table, $currentRows);

        return "Inserted 1 row.";
    }

    // --- 3. SELECT (Where & Joins) ---
    private function select($sql) {
        // Quick Parser for: SELECT * FROM t1 [JOIN t2 ON c1=c2] [WHERE col=val]

        // 1. Parse basics
        preg_match('/SELECT (.*?) FROM (\w+)(.*)/i', $sql, $matches);
        $table = $matches[2];
        $rest = $matches[3] ?? '';

        $rows = null;
        $schema = $this->storage->loadSchema($table); // Keep schema for validation if needed

        // CHECK FOR OPTIMIZATION: WHERE column = value
        if (preg_match('/WHERE (\w+)\s*=\s*(.+)/i', $rest, $wMatches)) {
            $col = $wMatches[1];
            $val = trim($wMatches[2], " '\"");

            // IF INDEX EXISTS -> USE IT (O(1) Lookup)
            if ($this->indexer->hasIndex($table, $col)) {
                $rows = $this->indexer->get($table, $col, $val);
                // We found our rows fast! Skip the full table scan.
                // Note: If we have other WHERE conditions, we'd filter this result further.
                return $this->formatResult($rows);
            }
        }

        // Fallback: Full Table Scan (O(N))
        if ($rows === null) {
            $rows = $this->storage->selectAll($table);
        }

        // 2. Handle JOIN
        if (preg_match('/JOIN (\w+) ON ([\w\.]+)\s*=\s*([\w\.]+)/i', $rest, $jMatches)) {
            $table2 = $jMatches[1];
            $col1 = explode('.', $jMatches[2])[1]; // Assume table.col format
            $col2 = explode('.', $jMatches[3])[1];

            $rows2 = $this->storage->selectAll($table2);
            $joinedRows = [];

            // Nested Loop Join (Simple but O(N*M))
            foreach ($rows as $r1) {
                foreach ($rows2 as $r2) {
                    if ($r1[$col1] == $r2[$col2]) {
                        // Merge rows. Keys might clash, so we prefix in a real DB. 
                        // Here we just merge arrays.
                        $joinedRows[] = array_merge($r1, $r2);
                    }
                }
            }
            $rows = $joinedRows;
        }

        // 3. Handle WHERE (Simple Equality only for demo)
        if (preg_match('/WHERE (\w+)\s*=\s*(.+)/i', $rest, $wMatches)) {
            $col = $wMatches[1];
            $val = trim($wMatches[2], " '\"");
            
            $filtered = [];
            foreach ($rows as $row) {
                if (isset($row[$col]) && $row[$col] == $val) {
                    $filtered[] = $row;
                }
            }
            $rows = $filtered;
        }

        return $this->formatResult($rows);
    }

    // --- 4. UPDATE ---
   private function update($sql) {
        // UPDATE users SET name='Bob' WHERE id=1
        if (!preg_match('/UPDATE (\w+) SET (\w+)=(.+?) WHERE (\w+)=(.+)/i', $sql, $m)) {
            throw new Exception("Syntax Error or complex UPDATE not supported");
        }
        $table = $m[1]; 
        $setCol = $m[2]; 
        $setVal = trim($m[3], " '\""); 
        $whereCol = $m[4]; 
        $whereVal = trim($m[5], " '\"");

        $schema = $this->storage->loadSchema($table);
        $rows = $this->storage->selectAll($table);
        $count = 0;
        
        // 1. Check Type Validity for the new value
        if (isset($schema[$setCol])) {
             if ($schema[$setCol]['type'] === 'INT' && !is_numeric($setVal)) 
                 throw new Exception("Type Error: $setCol must be INT");
        }

        foreach ($rows as $k => &$row) {
            if (isset($row[$whereCol]) && $row[$whereCol] == $whereVal) {
                
                // 2. CONSTRAINT CHECK (The Fix)
                if (isset($schema[$setCol]['unique']) || isset($schema[$setCol]['pk'])) {
                    foreach ($rows as $otherKey => $otherRow) {
                        // Check if ANY other row has this value
                        if ($k !== $otherKey && isset($otherRow[$setCol]) && $otherRow[$setCol] == $setVal) {
                            throw new Exception("Constraint Violation: $setCol must be unique. '$setVal' already exists.");
                        }
                    }
                }

                $row[$setCol] = $setVal;
                $count++;
            }
        }
        
        if ($count > 0) {
            $this->storage->saveRows($table, $rows);
            // Optimization: In a real DB, we would update the Index here too
            $this->updateIndex($table, $rows); 
        }
        
        return "Updated $count rows.";
    }

    // --- 5. DELETE ---
    private function delete($sql) {
        // DELETE FROM users WHERE id=1
        if (!preg_match('/DELETE FROM (\w+) WHERE (\w+)=(.+)/i', $sql, $m)) {
            throw new Exception("Syntax Error in DELETE");
        }
        $table = $m[1]; $whereCol = $m[2]; $whereVal = trim($m[3], " '\"");

        $rows = $this->storage->selectAll($table);
        $newRows = [];
        $count = 0;

        foreach ($rows as $row) {
            if ($row[$whereCol] == $whereVal) {
                $count++; // Skip this row (delete)
            } else {
                $newRows[] = $row;
            }
        }
        $this->storage->saveRows($table, $newRows);
        return "Deleted $count rows.";
    }

    // Helper to keep indexes fresh
    private function updateIndex($table, $rows) {
        $schema = $this->storage->loadSchema($table);
        foreach ($schema as $col => $def) {
            // Only update if an index file exists (PKs/Unique)
            if ($this->indexer->hasIndex($table, $col)) {
                $this->indexer->rebuild($table, $col, $rows);
            }
        }
    }

    private function formatResult($rows) {
        if (empty($rows)) return "Empty set.";
        $headers = array_keys(reset($rows));
        
        // Print Header
        $out = implode(" | ", $headers) . "\n" . str_repeat("-", 20) . "\n";
        
        // Print Rows
        foreach ($rows as $row) {
            $out .= implode(" | ", $row) . "\n";
        }
        return $out;
    }
}