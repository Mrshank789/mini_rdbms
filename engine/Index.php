<?php

class Index
{
    private $dir;

    public function __construct() {
        $this->dir = __DIR__ . '/../data/indexes/';
        if (!is_dir($this->dir)) mkdir($this->dir, 0777, true);
    }

    public function hasIndex($table, $col) {
        return file_exists($this->dir . "{$table}_{$col}.json");
    }

    public function get($table, $col, $value) {
        $data = json_decode(file_get_contents($this->dir . "{$table}_{$col}.json"), true);
        return $data[$value] ?? [];
    }

    // Rebuilds the index file entirely (Simplest strategy for this project)
    public function rebuild($table, $col, $rows) {
        $indexData = [];
        foreach ($rows as $row) {
            if (isset($row[$col])) {
                $val = $row[$col];
                // We store the full row for a "Covering Index" (Fast Reads)
                // In a real DB, we would store a pointer/ID.
                $indexData[$val][] = $row; 
            }
        }
        file_put_contents($this->dir . "{$table}_{$col}.json", json_encode($indexData));
    }
}