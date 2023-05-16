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

        $data_for_ydb = array(
            'column1' => 'some_record_where_we_try_to_find'
        );
        $test_arr = $this->ydb_v2->selectPythonYDB_v4('any_table_in_table_scheme', $data_for_ydb, 'equal', null, null);
        $test = $test_arr[0] ?? "";
        var_dump(json_encode($test));

        // $this->response = !empty ($this->response) ? $this->response : '{"status":"error", "error":"xxxx", "code":"80"}';

        // $data_for_ydb['column1'] = "text1 equal to type of column1 in scheme";
        // $data_for_ydb['column2'] = "text2 equal to type of column2 in scheme";
        // $save_res = $this->ydb_v2->savePythonYDB('table2', $data_for_ydb);
        // var_dump(json_encode($save_res));
    }
}
