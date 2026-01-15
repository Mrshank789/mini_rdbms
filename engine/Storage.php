<?php

class Storage
{
    public $dataDir;
    public $schemaDir;
    
    // Transaction buffers
    private $inTransaction = false;
    private $dataBuffer = []; 

    public function __construct()
    {
        $this->dataDir = __DIR__ . '/../data/tables/';
        $this->schemaDir = __DIR__ . '/../data/schemas/';
        
        if (!is_dir($this->dataDir)) mkdir($this->dataDir, 0777, true);
        if (!is_dir($this->schemaDir)) mkdir($this->schemaDir, 0777, true);
    }

    // --- SCHEMA MANAGEMENT ---
    public function saveSchema($table, $schema) {
        file_put_contents($this->schemaDir . $table . '.json', json_encode($schema, JSON_PRETTY_PRINT));
    }

    public function loadSchema($table) {
        $file = $this->schemaDir . $table . '.json';
        if (!file_exists($file)) throw new Exception("Table '$table' does not exist.");
        return json_decode(file_get_contents($file), true);
    }

    // --- DATA MANAGEMENT (Transaction Aware) ---
    public function beginTransaction() {
        if ($this->inTransaction) throw new Exception("Transaction already active.");
        $this->inTransaction = true;
        $this->dataBuffer = [];
        return "Transaction Started";
    }

    public function commit() {
        if (!$this->inTransaction) throw new Exception("No active transaction.");
        foreach ($this->dataBuffer as $table => $rows) {
            $this->saveTableToDisk($table, $rows);
        }
        $this->inTransaction = false;
        $this->dataBuffer = [];
        return "Transaction Committed";
    }

    public function rollback() {
        if (!$this->inTransaction) throw new Exception("No active transaction.");
        $this->inTransaction = false;
        $this->dataBuffer = [];
        return "Transaction Rolled Back";
    }

    public function selectAll($table) {
        // Return buffered data if valid, otherwise read from disk
        if ($this->inTransaction && isset($this->dataBuffer[$table])) {
            return $this->dataBuffer[$table];
        }
        return $this->loadTableFromDisk($table);
    }

    public function saveRows($table, $rows) {
        if ($this->inTransaction) {
            $this->dataBuffer[$table] = $rows;
        } else {
            $this->saveTableToDisk($table, $rows);
        }
    }

    // --- LOW LEVEL I/O ---
    private function loadTableFromDisk($table) {
        $file = $this->dataDir . $table . '.json';
        return file_exists($file) ? json_decode(file_get_contents($file), true) : [];
    }

    // Changed to PUBLIC as per your fix request
    public function saveTableToDisk($table, $data) {
        $file = $this->dataDir . $table . '.json';
        file_put_contents($file . '.tmp', json_encode($data, JSON_PRETTY_PRINT));
        rename($file . '.tmp', $file);
    }
}