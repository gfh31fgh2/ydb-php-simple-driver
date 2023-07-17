<?php

namespace xxxx;

// use xxxx\Logger;
use YdbPlatform\Ydb\Ydb;

class YDBFunctions_v2 {

    private static $_instance = null;
    private static $_ydb;
    private static $_ydbsession;
    private static $session_info;

    public static function getInstance()
    {
        if (!self::$_instance) {
            self::$_instance = new YDBFunctions_v2;
        }
        return self::$_instance;
    }

    public static function getYDB($newone = false)
    {
        // Если требуется новый коннект
        if ($newone) {
            // Logger::add_msg("INFO: YDBFv2: [" . __FUNCTION__ . "]: newone true ");
            self::$_ydb = self::getInstance()->connectYDB(true);
        } else {
            if (!self::$_ydb) {
                self::$_ydb = self::getInstance()->connectYDB();
            } else {
                // Logger::add_msg("INFO: YDBFv2: [" . __FUNCTION__ . "]: Already have ydb_instance ");
            }
        }

        return self::$_ydb;
    }

    public static function getYdbSession($ydb_retry)
    {
        if ($ydb_retry > 2) {
            // Logger::add_msg("INFO: YDBFv2: [" . __FUNCTION__ . "]: getYdbSession_ydb_retry > 2 and = " . $ydb_retry);
            // $token_update = self::getYDB()->token();
            // Logger::add_msg("INFO: YDBFv2: [". __FUNCTION__ . "]: ydb_update_token = " . json_encode($token_update) );
            self::$_ydbsession = self::getYDB(true)->table()->session();
            self::$session_info = "new conn";

        } else {
            if (!self::$_ydbsession) {
                self::$_ydbsession = self::getYDB()->table()->session();
                self::$session_info = "new conn";
            } else {
                // Logger::add_msg("INFO: YDBFv2: [" . __FUNCTION__ . "]: Already have _ydbsession ");
                self::$session_info = "already conn";
            }
        }

        return self::$_ydbsession;
    }

    public function __construct()
    {
        // Logger::add_msg("INFO: YDBFv2: [" . __FUNCTION__ . "]: Construction YDBFv2... ");
        return;
    }

    public function connectYDB()
    {
        $config_db          = CONFIG_YDB_DATABASE;
        $config_endpoint    = CONFIG_YDB_ENDPOINT;
        $config_sa_pathfile = CONFIG_YDB_SA_PATH_FILE;
        $config_tmp         = CONFIG_YDB_PATH_TMP;
        $config_ssl_ya      = CONFIG_YDB_PATH_SSL_YA;

        // Logger::add_msg("INFO: YDBFv2: [". __FUNCTION__ . "]: config_tmp = " . $config_tmp);

        $config = [
            'database'    => $config_db,
            'endpoint'    => $config_endpoint,
            'discovery'   => false,
            'iam_config'  => [
                'temp_dir'       => $config_tmp,
                'service_file'   => $config_sa_pathfile,
                'root_cert_file' => $config_ssl_ya
            ],
        ];


        $already = false;
        if ( is_null(self::$_ydb)) {
            try {
                // self::$_ydb = new Ydb($config, new Logg());
                self::$_ydb = new Ydb($config);
                // Logger::add_msg("INFO: YDBFv2: [" . __FUNCTION__ . "]: New connection!");
            } catch (\Exception $e) {
                // Logger::add_msg("ERR: YDBFv2: [". __FUNCTION__ . "]: Error, catch: " . $e->getMessage() );
                sleep(1);
            }
        } else {
            // Logger::add_msg("INFO: YDBFv2: [". __FUNCTION__ . "]: Already connection!");
            $already = true;
        }
        return self::$_ydb;
    }

    public function helperForTypes($type = null, $value = null)
    {
        if ($type == 'Utf8') {
            $mod_value = strval($value);
            return $mod_value;
        }

        if ($type == 'Int32') {
            $mod_value = intval($value);
            return $mod_value;
        }

        if ($type == 'Json') {
            $mod_value = strval($value);
            return $mod_value;

        $mod_value = strval($value);
        return $mod_value;
    }


    public function selectPythonYDB_v4($nametable, $data_arr, $operand, $limit = null, $orderby = null,  $special_index = null, $need_count = false ) {
        $path_table = CONFIG_YDB_DATABASE . '/';

        $answ_from_ydb_sdk_php = self::select_ydb($path_table, $nametable, $operand, $limit, $orderby, $data_arr, 2, $special_index, $need_count);

        // Logger::add_msg("INFO: YDBFv2: [" . __FUNCTION__ . "]: ydb3005, answ_from_ydb_sdk_php = " . json_encode($answ_from_ydb_sdk_php));

        return ($answ_from_ydb_sdk_php['selectdata']);
    }


    public function select_ydb($path_table = null, $nametable = null, $operand = null, $limit = null, $orderby_arr = null, $selectdata = null, $retries = 2, $special_index = null, $need_count = false) 
    {
        // Проверяем входящие переменные
        if (!$path_table) {
            // Logger::add_msg("ERR: YDBFv2: [". __FUNCTION__ . "]: path_table not isset ");
            return false;
        }

        if (!$nametable) {
            // Logger::add_msg("ERR: YDBFv2: [". __FUNCTION__ . "]: nametable not isset ");
            return false;
        }

        if (!$operand) {
            // Logger::add_msg("ERR: YDBFv2: [". __FUNCTION__ . "]: operand not isset ");
            return false;
        }

        if (!$limit) {
            // Logger::add_msg("INFO: YDBFv2: [". __FUNCTION__ . "]: limit not isset ");
            // return false;
        }

        if (!$orderby_arr) {
            // Logger::add_msg("INFO: YDBFv2: [". __FUNCTION__ . "]: orderby_arr not isset ");
            // return false;
        }

        if (!$selectdata) {
            // Logger::add_msg("ERR: YDBFv2: [". __FUNCTION__ . "]: selectdata not isset ");
            return false;
        }

        // Logger::add_msg("INFO: YDBFv2: [". __FUNCTION__ . "]: nametable = " . $nametable);
        // Logger::add_msg("INFO: YDBFv2: [". __FUNCTION__ . "]: operand = " . $operand);
        // Logger::add_msg("INFO: YDBFv2: [". __FUNCTION__ . "]: limit = " . $limit);
        // Logger::add_msg("INFO: YDBFv2: [". __FUNCTION__ . "]: orderby_arr = " . json_encode($orderby_arr));
        // Logger::add_msg("INFO: YDBFv2: [". __FUNCTION__ . "]: selectdata = " . json_encode($selectdata));
        // Logger::add_msg("INFO: YDBFv2: [". __FUNCTION__ . "]: special_index = " . $special_index);

        if ($operand == 'equal') {
            foreach ($selectdata as $key => $value) {
                $xxxxColumn = $key;
                $xxxxValue  = $value;
            }
            $select_result = $this->select_eq($path_table, $nametable, $xxxxColumn, $xxxxValue, $limit, $orderby_arr, $special_index, $need_count);
        } elseif($operand == 'query') {
            foreach ($selectdata as $key => $value) {
                if ($key == 'query') {
                    $ydb_query = $value;
                }
            }
            $select_result = $this->select_query($path_table, $nametable, $ydb_query);
        }

        // Logger::add_msg("INFO: YDBFv2: [". __FUNCTION__ . "]: select_result = " . json_encode($select_result) );
        
        return $select_result['data'];
    }




    // ========================  SELECT_EQ  ========================  //

    // EQUAL
    // {
    //     "table": "xxxx",
    //     "operand": "equal",
    //     "limit": "10",
    //     "orderby": {
    //         "asc": "DESC",
    //         "key": "xxxxx"
    //     },
    //     "data": {
    //         "column": "record"
    //     }
    // }

    public function select_eq($path, $xxxxTable, $xxxxColumn, $xxxxValue, $xxxxLimit, $xxxxOrderBy, $xxxSpecialIndex, $need_count) {
        
        // Определяем тип переменной
        $scheme_table_arr = self::xxxx_scheme_table($xxxxTable);
        $scheme_table  = $scheme_table_arr[0];
        $indexes_table = $scheme_table_arr[1];
        $xxxxValueType = $scheme_table[$xxxxColumn];

        $prepared_query_text = '';
        $query1 = 'PRAGMA TablePathPrefix(' . '"' . $path . '"' . ');' . "\n";
        $query2 = 'DECLARE $xxxxValue AS ' . $xxxxValueType . ';' . "\n";
        
        // Проверяем, есть ли индекс по этой колонке в нашей схеме
        if ($need_count) {
            $query3 = 'SELECT COUNT(*) FROM `' . $xxxxTable .'`' . ' VIEW ' . $xxxSpecialIndex . ' AS ost ' . ' WHERE ' . 'ost.' . $xxxxColumn . ' = ' . '$xxxxValue';
        } else {
            if ( isset($indexes_table[$xxxxColumn]) ) {
                $idx = $indexes_table[$xxxxColumn];
                $query3 = 'SELECT * FROM `' . $xxxxTable .'`' . ' VIEW ' . $idx . ' AS ost ' . ' WHERE ' . 'ost.' . $xxxxColumn . ' = ' . '$xxxxValue';
            } else {
                $query3 = 'SELECT * FROM `' . $xxxxTable .'`' . ' WHERE ' . $xxxxColumn . ' = ' . '$xxxxValue';
            }
        }

        
        // orderby
        if (  (isset($xxxxOrderBy['asc'])) && (isset($xxxxOrderBy['key'])) ) {
            if ($xxxxOrderBy['asc'] == 'DESC') {
                $ord = 'DESC';
            } elseif($xxxxOrderBy['asc'] == 'ASC') {
                $ord = 'ASC';
            } else {
                $ord = 'DESC';    
            }
            $query4 = "\n" . ' ORDER BY `' . strval($xxxxOrderBy['key']) . '` ' . $ord;
        } else {
            $query4 = '';
        }

        // limit
        if (  !empty($xxxxLimit) ) {
            $query5 = "\n" . ' LIMIT ' . intval($xxxxLimit);
        } else {
            $query5 = '';
        }
        $query6 = ';';
        $prepared_query_text = $query1 . $query2 . $query3 . $query4 . $query5 . $query6;

        // Logger::add_msg("INFO: YDBFv2: [". __FUNCTION__ . "]: prepared_query_text = " . $prepared_query_text);

        $st = 0;
        while ($st < 5) {
            $st = $st + 1;
            // Logger::add_msg("INFO: YDBFv2: [". __FUNCTION__ . "]: ydb_retry=" . $st );

            try {
                // $session = self::getYDB()->table()->session();
                $session = self::getYdbSession($st);
                $prepared_query = $session->prepare($prepared_query_text);

                $res = $session->transaction(function($session) use ($xxxxValue, $prepared_query){
                    return $prepared_query->execute([
                        'xxxxValue' => $xxxxValue,
                    ]);
                });

                // Logger::add_msg("INFO: YDBFv2: [". __FUNCTION__ . "]: result_ydb = " . json_encode($res));
                $return_data = [
                    "status" => "success",
                    "code"   => "100",
                    "msg"    => "ok",
                    "conn"   => self::$session_info,
                    "data"   => $res->rows()
                ];
                $st = 20;
            } catch (\Exception $e) {
                // Logger::add_msg("INFO: YDBFv2: [". __FUNCTION__ . "]: Error, catch: " . $e->getMessage() );
                sleep(1);
                $return_data = [
                    "status"    => "error",
                    "code"      => "101", 
                    "msg"       => "error with ydb",
                    "moreinfo"  => $e->getMessage(),
                    "data"      => ""
                ];
            }
        }

        return $return_data;
    }
    // ========================  END SELECT_EQ  ========================  //


    // ========================  SELECT_QUERY  ========================  //

    public function select_query($path, $xxxTable, $xxxQuery) {
        if (!$xxxTable) {
            $return_data = [
                "status"    => "error",
                "code"      => "110",
                "msg"       => "xxxTable not isset",
                "data"      => ""
            ];
            // Logger::add_msg("ERR: YDBFv2: [". __FUNCTION__ . "]: xxxTable not isset ");
            return $return_data;
        }
        // Logger::add_msg("INFO: YDBFv2: [". __FUNCTION__ . "]: xxxTable = " . $xxxTable);

        if (!$xxxQuery) {
            $return_data = [
                "status"    => "error",
                "code"      => "111",
                "msg"       => "xxxQuery not isset",
                "data"      => ""
            ];
            // Logger::add_msg("ERR: YDBFv2: [". __FUNCTION__ . "]: xxxQuery not isset");
            return $return_data;
        }
        // Logger::add_msg("INFO: YDBFv2: [". __FUNCTION__ . "]: xxxQuery = " . json_encode($xxxQuery));

        // Определяем тип переменных
        $scheme_table_arr = self::nano_scheme_table($xxxTable);
        $scheme_table  = $scheme_table_arr[0];
        $indexes_table = $scheme_table_arr[1];
        // $nanoValueTypeLT = $scheme_table[$nanoValueLT];

        $prepared_query_text = '';
        $query1 = 'PRAGMA TablePathPrefix(' . '"' . $path . '"' . ');' . "\n";
        // $query21 = 'DECLARE $nanoValueLT AS ' . $nanoValueTypeLT . ';' . "\n";
        // $query22 = '';

        // Проверяем, есть ли индекс по этой колонке в нашей схеме
        // $query31 = 'SELECT * FROM `' . $xxxTable .'`' . ' WHERE `' . $nanoKeyColumn . '` < ' . '$nanoValueLT';
        $query31 = $xxxQuery;

        $prepared_query_text = $query1 . $query31 ;

        // Logger::add_msg("INFO: YDBFv2: [". __FUNCTION__ . "]: prepared_query_text = " . $prepared_query_text);

        $st = 0;
        while ($st < 5) {
            $st = $st + 1;
            // Logger::add_msg("INFO: YDBFv2: [". __FUNCTION__ . "]: ydb_retry=" . $st );
            try {
                $session = self::getYdbSession($st);
                $prepared_query = $session->prepare($prepared_query_text);

                if ($prepared_query == false) {
                    // Logger::add_msg("WARN: YDBFv2: [". __FUNCTION__ . "]: ydb_retry_res = false!");
                } else {
                    // $res = $session->transaction(function($session) use ($prepared_query){
                    //     return $prepared_query->execute();
                    // });

                    $res = self::getYDB()->table()->retryTransaction(function($session) use ($prepared_query){
                        return $prepared_query->execute();
                    }, false);

                    // Logger::add_msg("INFO: YDBFv2: [". __FUNCTION__ . "]: result_ydb_with_retry_m1 = " . json_encode($res));
                    $return_data = [
                        "status" => "success",
                        "code"   => "100",
                        "msg"    => "ok",
                        "conn"   => self::$session_info,
                        "data"   => $res->rows()
                    ];
                    $st = 20;
                }

            } catch (\Exception $e) {
                // Logger::add_msg("INFO: YDBFv2: [". __FUNCTION__ . "]: Error, catch: " . $e->getMessage() );
                sleep(1);
                $return_data = [
                    "status"    => "error",
                    "code"      => "105", 
                    "msg"       => "error with ydb",
                    "moreinfo"  => $e->getMessage(),
                    "data"      => ""
                ];
            }
        }

        return $return_data;

    }
    // ========================  END SELECT_QUERY  ========================  //




    // ========================  UPSERT  ========================  //

    // EQUAL
    // {
    //     "path_table": "/ru-xxxx/sadsa/xxxxxx",
    //     "table": "nametable",
    //     "putdata_arr": array (
    //                      "column1" => "asdsadas",
    //                      "somecolumn2" => 123,
    //                   )
    // }

    public function upsert($path_table, $nametable, $putdata_arr) {

        if (!$nametable) {
            $return_data = [
                "status"    => "error",
                "code"      => "110",
                "msg"       => "nametable not isset",
                "data"      => ""
            ];
            // Logger::add_msg("ERR: YDBFv2: [". __FUNCTION__ . "]: nametable not isset ");
            return $return_data;
        }
        // Logger::add_msg("INFO: YDBFv2: [". __FUNCTION__ . "]: nametable = " . $nametable);

        if (!$putdata_arr) {
            $return_data = [
                "status"    => "error",
                "code"      => "111",
                "msg"       => "putdata_arr not isset",
                "data"      => ""
            ];
            // Logger::add_msg("ERR: YDBFv2: [". __FUNCTION__ . "]: putdata_arr not isset");
            return $return_data;
        }
        // Logger::add_msg("INFO: YDBFv2: [". __FUNCTION__ . "]: putdata_arr = " . json_encode($putdata_arr));
        
        $putdata_modified = array();
        $scheme_table_arr = self::xxxx_scheme_table($nametable);
        $scheme_table  = $scheme_table_arr[0];
        $indexes_table = $scheme_table_arr[1];

        foreach ($scheme_table as $scheme_table_key => $scheme_table_type) {
            if ( isset($putdata_arr[$scheme_table_key]) ) {
                $putdata_modified[$scheme_table_key] = $this->helperForTypes($scheme_table_type, $putdata_arr[$scheme_table_key]);
            }
        }

        if ( empty($putdata_modified) )  {
            // Log::error("ERR: YDBFv2: [". __FUNCTION__ . "]: putdata_modified empty! ");
            return false;
        }

        $declares_text = '';
        $declares_text = $declares_text . 'DECLARE $rowData AS List<Struct<';
        $st = 0;
        foreach ($scheme_table as $namecolumn => $typecolumn) {
            if ($st == 0) {
                if (isset($putdata_modified[$namecolumn])) {
                    if ($typecolumn == 'Timestamp') {
                        $declares_text = $declares_text . "\n " .  $namecolumn . ': ' . 'Uint64';
                    } else {
                        $declares_text = $declares_text . "\n " .  $namecolumn . ': ' . $typecolumn;
                    }
                }
            } else {
                if (isset($putdata_modified[$namecolumn])) {
                    if ($typecolumn == 'Timestamp') {
                       $declares_text = $declares_text . ',' . "\n " .  $namecolumn . ': ' . 'Uint64' ;
                    } else {
                        $declares_text = $declares_text . ',' . "\n " .  $namecolumn . ': ' . $typecolumn ;
                    }
                }
            }
            $st++;
        }
        $declares_text = $declares_text . '>>;';

        // $putdata ex:
        // array(
        //     "pk_b"          => "testpkb",
        //     "name"          => "testpkb",
        //     "numb_clmn"     => 33,
        //     "descr"         => "asdassa"
        // );

        $only_columns_text = '';
        $st = 0;
        foreach ($putdata_modified as $putdata_column => $putdata_value) {
            if ($st == 0) {
                if ($scheme_table[$putdata_column] == 'Timestamp') {
                    $only_columns_text = $only_columns_text . 'CAST(' . $putdata_column . ' AS Optional<Timestamp>) AS ' . $putdata_column;
                } else {
                    $only_columns_text = $only_columns_text . $putdata_column;
                }
            } else {
                if ($scheme_table[$putdata_column] == 'Timestamp') {
                    $only_columns_text = $only_columns_text . ', ' . 'CAST(' . $putdata_column . ' AS Optional<Timestamp>) AS ' . $putdata_column;
                } else {
                    $only_columns_text = $only_columns_text . ', ' . $putdata_column;
                }
            }
            $st++;
        }
        // Logger::add_msg("INFO: YDBFv2: [". __FUNCTION__ . "]: only_columns_text = " . $only_columns_text);

        $rowData = array($putdata_modified);

        // Logger::add_msg("INFO: YDBFv2: [". __FUNCTION__ . "]: rowData = " . json_encode($rowData));

        $upsert_text = '';
        $upsert_text = $upsert_text . 'UPSERT INTO `' . $nametable . '`' ;
        $upsert_text = $upsert_text . ' SELECT ' . $only_columns_text . ' FROM AS_TABLE($rowData);';

        $prepared_query_text = 'PRAGMA TablePathPrefix("' . $path_table . '");' . "\n" . $declares_text . "\n"  . $upsert_text;

        // Logger::add_msg("INFO: YDBFv2: [". __FUNCTION__ . "]: prepared_query_text = " . $prepared_query_text);

        $st = 0;
        while ($st < 5) {
            $st = $st + 1;
            // Logger::add_msg("INFO: YDBFv2: [". __FUNCTION__ . "]: ydb_retry=" . $st );

            try {
                // проблемы с этим
                $session = self::getYDB()->table()->session();

                // а это возможно исправит косяки sdk:
                // $session = self::getYdbSession($st);

                $prepared_query = $session->prepare($prepared_query_text);

                $res = $session->transaction(function($session) use ($rowData, $prepared_query){
                    return $prepared_query->execute([
                        'rowData' => $rowData,
                    ]);
                });

                // Logger::add_msg("INFO: YDBFv2: [". __FUNCTION__ . "]: result_ydb = " . json_encode($res));
                $return_data = [
                    "status" => "success",
                    "code"   => "100",
                    "msg"    => "ok",
                    "conn"   => self::$session_info,
                    "data"   => $res->rows()
                ];
                $st = 20;
            } catch (\Exception $e) {
                // Logger::add_msg("INFO: YDBFv2: [". __FUNCTION__ . "]: Error, catch: " . $e->getMessage() );
                $return_data = [
                    "status"    => "error",
                    "code"      => "112", 
                    "msg"       => "error with ydb",
                    "moreinfo"  => $e->getMessage(),
                    "data"      => ""
                ];
            }
        }

        return $return_data;

    }
    // ========================  END UPSERT  ========================  //




    public function __wakeup() {}
    private function __clone() {}

    public function __destruct()
    {
        // Logger::add_msg("INFO: YDBFv2: [". __FUNCTION__ . "]: Destruction YDBFv2... ");
    }

    static function xxxx_scheme_table($tablename = null) {

        if ($tablename == 'nametable') {
            $scheme_arr = array(
                self::xxxx_scheme_nametable(), 
                self::xxxx_indexes_nametable()
            );
            return $scheme_arr;
        }
        return false;
    }

    static function xxxx_scheme_nametable() {
        $list = array(
            "column1" => "Utf8",  
            "column2" => "Int32",  
            "column3" => "Timestamp"
        );
        return ($list);
    }

    static function xxxx_indexes_nametable() {
        $indexes = array(
            'column3'  => 'idx_tstamp'
        );
        return ($indexes);
    }
    
} // end class
