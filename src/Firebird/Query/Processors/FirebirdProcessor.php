<?php namespace Firebird\Query\Processors;

use Illuminate\Database\Query\Processors\Processor;
use Illuminate\Database\Query\Builder;
use Illuminate\Support\Facades\DB;


class FirebirdProcessor extends Processor {
  
  
    /**
     * 
     * Simply workaround for the error 
     * "SQLSTATE[IM001]: Driver does not support this function: driver does not support lastInsertId()"
     * Assuming that the primary key column is integer
     * 
     * 
     * Process an  "insert get ID" query.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  string  $sql
     * @param  array   $values
     * @param  string  $sequence
     * @return int
     */
    public function processInsertGetId(Builder $query, $sql, $values, $sequence = null) {
        $query->getConnection()->insert($sql, $values);
        $field_name = $this->getFirebirdPrimaryKeyFieldName($query);
        $id = $id = DB::table($query->from)->max($field_name);
        return is_numeric($id) ? (int) $id : $id;
    }
    
    /**
     * 
     * @param Builder $query
     * @return integer
     */
    private function getFirebirdPrimaryKeyFieldName(Builder $query){
      
      $result = DB::select("SELECT idx.RDB\$FIELD_NAME as FIELD_NAME FROM RDB\$RELATION_CONSTRAINTS relc join RDB\$INDEX_SEGMENTS idx on idx.RDB\$INDEX_NAME = relc.RDB\$INDEX_NAME WHERE relc.RDB\$RELATION_NAME = ? and relc.RDB\$CONSTRAINT_TYPE = 'PRIMARY KEY'", array($query->from));
      if (!isset($result[0])) {
        throw new InvalidArgumentException('Primary key field was not found');
      } else {
        return $result[0]->FIELD_NAME;
      }
      
    }
    
    /**
     * 
     * Return a table Foreign key list
     * 
     * @param Builder $query
     * @return array
     */
    private function getFirebirdForeignKeyFields(Builder $query) {
      
      $result = DB::select("SELECT
      is1.RDB\$FIELD_NAME as FOREIGN_KEY,
      is2.RDB\$FIELD_NAME as REFERENCE_FIELD,
      relc2.RDB\$RELATION_NAME as REFERENCE_TABLE
      FROM
      RDB\$RELATION_CONSTRAINTS relc1
      join RDB\$REF_CONSTRAINTS refc on refc.RDB\$CONSTRAINT_NAME = relc1.RDB\$CONSTRAINT_NAME
      join RDB\$RELATION_CONSTRAINTS relc2 on relc2.RDB\$CONSTRAINT_NAME = refc.RDB\$CONST_NAME_UQ
      join RDB\$INDEX_SEGMENTS is1 on is1.RDB\$INDEX_NAME = relc1.RDB\$INDEX_NAME
      join RDB\$INDEX_SEGMENTS is2 on is2.RDB\$INDEX_NAME = relc2.RDB\$INDEX_NAME
      WHERE
      relc1.RDB\$RELATION_NAME = ? and
      relc1.RDB\$CONSTRAINT_TYPE = 'FOREIGN KEY' and
      relc2.RDB\$CONSTRAINT_TYPE = 'PRIMARY KEY'", array($query->from));
      
      return $result;
      
    }
    
}
