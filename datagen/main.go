package main

import (
	"fmt"
	"math/rand"

	"github.com/zyguan/sql-spider/util"
)

func main() {
	dropTable := `
DROP TABLE IF EXISTS t;`
	createTable := `
CREATE TABLE t (
	col_int int default null,
	col_double double default null,
	col_decimal decimal(40, 20) default null,
	col_string varchar(40) default null,
	col_datetime datetime default null,
	key(col_int),
	key(col_double),
	key(col_decimal),
	key(col_string),
	key(col_datetime),
	key(col_int, col_double),
	key(col_int, col_decimal),
	key(col_int, col_string),
	key(col_double, col_decimal)
);`
	fmt.Println(dropTable)
	fmt.Println(createTable)
	n := 100000
	for i := 0; i < n; i++ {
		insert := fmt.Sprintf(`INSERT IGNORE INTO t values (%v, %v, %v, %v, %v);`,
			optional(.9, util.GenIntLiteral), optional(.9, util.GenRealLiteral), optional(.9, util.GenRealLiteral),
			optional(.9, util.GenStringLiteral), optional(.9, util.GenDateTimeLiteral))
		fmt.Println(insert)
	}
	fmt.Println(`analyze table t;`)
}

func optional(p float64, f func() string) string {
	if rand.Float64() < p {
		return f()
	}
	return "NULL"
}
