# ydb-php-simple-driver

Simple driver based on ydb-php-sdk
1) добавить необходимый namespace
2) pecl install grpc
3) composer update/install
4) добавить данные в config
5) добавить схему таблицы в ydbf_v2 (xxxx_scheme_nametable)
6) добавить схему индексов таблицы в ydbf_v2 (xxxx_indexes_nametable)
7) в testone сделать необходимое действие (selectPythonYDB_v4 или savePythonYDB)

ps
Logger если нужно сами сделайте как удобно
типы данных обработку тоже как считаете нужным
другие типы вз-я с бд (between и др) - можете сами аналогично реализовать

