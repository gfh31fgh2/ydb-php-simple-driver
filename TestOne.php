<?php namespace xxxx;

// use xxxx\Logger;
use xxxx\YDBFunctions_v2;

class TestOne
{
    // public $response;
    protected $ydb_v2;
    
    public function __construct($data)
    {
        $this->ydb_v2 = new YDBFunctions_v2();
    }

    public function run()
    {

        $test_arr = $this->ydb_v2->selectPythonYDB_v4('kkt_status', $data_for_ydb, 'equal', null, null);
        $test = $test_arr[0] ?? "";
        var_dump(json_encode($test));

        // $this->response = !empty ($this->response) ? $this->response : '{"status":"error", "error":"xxxx", "code":"80"}';

        // $save_res = $this->ydb_v2->savePythonYDB('table2', $data_for_ydb);
        // var_dump(json_encode($save_res));
    }
}
