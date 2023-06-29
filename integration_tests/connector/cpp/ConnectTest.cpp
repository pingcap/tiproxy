#include <iostream>
#include "mysql.h"

using namespace std;

int main(int argc, char *argv[]) {
    MYSQL mysql;
    MYSQL *conn;
    MYSQL_RES *resultset;
    MYSQL_ROW row;
    int ssl_mode = SSL_MODE_VERIFY_IDENTITY;

    mysql_init(&mysql);
    mysql_options(&mysql, MYSQL_OPT_SSL_MODE, &ssl_mode);
    mysql_options(&mysql, MYSQL_OPT_SSL_CA, argv[4]);
    conn = mysql_real_connect(&mysql, argv[1], argv[2], argv[3], "test", 4000, NULL, 0);

    mysql_query(conn, "SHOW DATABASES");
    resultset = mysql_store_result(conn);
    while((row = mysql_fetch_row(resultset))){
        for (int i = 0; i < mysql_num_fields(resultset) ; i++)
            cout << (row[ i ]) << endl;
    }
    mysql_free_result(resultset);
    mysql_close(conn) ;
}
