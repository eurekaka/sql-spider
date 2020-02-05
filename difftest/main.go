package main

import (
	"bytes"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/ngaut/log"
	"github.com/zyguan/sql-spider/exprgen"
	"github.com/zyguan/sql-spider/nodegen"
	"github.com/zyguan/sql-spider/util"
)

var cntErrMismatch = 0
var cntErrNotReported = 0
var cntErrUnexpected = 0
var cntContentMismatch = 0

func getTableSchemas() util.TableSchemas {
	return util.TableSchemas{
		{Name: "t",
			Columns: []util.Column{
				util.NewColumn("col_int", util.ETInt),
				util.NewColumn("col_double", util.ETReal),
				util.NewColumn("col_decimal", util.ETDecimal),
				util.NewColumn("col_string", util.ETString),
				util.NewColumn("col_datetime", util.ETDatetime),
			}},
	}
}

func main() {
	var opts struct {
		mysql   string
		tidb    string
		trees   int
		queries int
	}
	flag.StringVar(&opts.mysql, "mysql", "root:@tcp(127.0.0.1:3306)/test", "mysql dsn")
	flag.StringVar(&opts.tidb, "tidb", "root:@tcp(127.0.0.1:4000)/spider", "tidb dsn")
	flag.IntVar(&opts.trees, "trees", 360, "number of tree")
	flag.IntVar(&opts.queries, "queries", 10, "queries per tree")
	flag.Parse()

	mydb, err := sql.Open("mysql", opts.mysql)
	perror(err)
	tidb, err := sql.Open("mysql", opts.tidb)
	perror(err)

	ts := getTableSchemas()
	emptyTrees := nodegen.GenerateNode(opts.trees)
	var trees []util.Tree
	for _, et := range emptyTrees {
		trees = append(trees, exprgen.GenExprTrees(et, ts, opts.queries)...)
	}

	r := Runner{mydb: mydb, tidb: tidb}
	r.errInconsistency, err = os.OpenFile("err_diff.out", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	perror(err)
	r.outInconsistency, err = os.OpenFile("out_diff.out", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	perror(err)
	r.consistency, err = os.OpenFile("query.sql", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	perror(err)

	for i, t := range trees {
		log.Info("#", i)
		r.Run(t)
	}
}

func perror(err error) {
	if err != nil {
		panic(err)
	}
}

type Runner struct {
	errInconsistency io.Writer
	outInconsistency io.Writer
	consistency io.Writer

	mydb *sql.DB
	tidb *sql.DB
}

var debug = true

func (r *Runner) Run(t util.Tree) {
	prep := t.ToBeautySQL(0)
	execs := t.ToExecuteStmts(10)
	if debug {
		fmt.Println(prep + ";")
		for _, e := range execs {
			fmt.Println(e)
		}
		return
	}
	ctx := context.Background()
	_, prepareExpErr := r.mydb.ExecContext(ctx, prep)
	_, prepareActErr := r.tidb.ExecContext(ctx, prep)
	if prepareExpErr != nil || prepareActErr != nil {
		log.Infof("prepare fails")
		return
	}
	prepareSuccLogged := false
	prepareFailLogged := false
	for i := 0; i < len(execs); i = i + 2 {
		_, setExpErr := r.mydb.ExecContext(ctx, execs[i])
		_, setActErr := r.tidb.ExecContext(ctx, execs[i])
		if setExpErr != nil || setActErr != nil {
			log.Infof("set fails %d", i)
			log.Error(setExpErr)
			log.Error(setActErr)
			continue
		}
		if debug {
			fmt.Println("index", i)
		}
		expRows, expErr := r.mydb.QueryContext(ctx, execs[i+1])
		if debug {
			fmt.Println("mysql query finishes")
		}
		actRows, actErr := r.tidb.QueryContext(ctx, execs[i+1])
		if expErr == nil && actErr == nil {
			expBR, err := dumpToByteRows(expRows)
			if err != nil {
				log.Infof("dump fails")
				if expRows != nil {
					expRows.Close()
				}
				if actRows != nil {
					actRows.Close()
				}
				continue
			}
			actBR, err := dumpToByteRows(actRows)
			if err != nil {
				log.Infof("dump fails")
				if expRows != nil {
					expRows.Close()
				}
				if actRows != nil {
					actRows.Close()
				}
				continue
			}
			if compareByteRows(expBR, actBR) {
				if !prepareSuccLogged {
					prepareSuccLogged = true
					r.consistency.Write([]byte("\n"))
					r.consistency.Write([]byte(prep))
					r.consistency.Write([]byte(";"))
					r.consistency.Write([]byte("\n"))
				}
				r.consistency.Write([]byte(execs[i]))
				r.consistency.Write([]byte("\n"))
				r.consistency.Write([]byte(execs[i+1]))
				r.consistency.Write([]byte("\n"))
				log.Infof("test query found")
			} else {
				if !prepareFailLogged {
					prepareFailLogged = true
					r.outInconsistency.Write([]byte("\n"))
					r.outInconsistency.Write([]byte(prep))
					r.outInconsistency.Write([]byte(";"))
					r.outInconsistency.Write([]byte("\n"))
				}
				r.outInconsistency.Write([]byte(execs[i]))
				r.outInconsistency.Write([]byte("\n"))
				r.outInconsistency.Write([]byte(execs[i+1]))
				r.outInconsistency.Write([]byte("\n"))
				log.Infof("different execute results %d", i)
			}
		}
		if expRows != nil {
			expRows.Close()
		}
		if actRows != nil {
			actRows.Close()
		}
	}
}

type byteRow struct {
	data [][]byte
}

type byteRows struct {
	types []*sql.ColumnType
	cols  []string
	data  []byteRow
}

func (rows *byteRows) Len() int {
	return len(rows.data)
}

func (rows *byteRows) Less(i, j int) bool {
	r1 := rows.data[i]
	r2 := rows.data[j]
	for i := 0; i < len(r1.data); i++ {
		res := bytes.Compare(r1.data[i], r2.data[i])
		switch res {
		case -1:
			return true
		case 1:
			return false
		}
	}
	return false
}

func (rows *byteRows) Swap(i, j int) {
	rows.data[i], rows.data[j] = rows.data[j], rows.data[i]
}

func (rows *byteRows) convertToString() string {
	res := strings.Join(rows.cols, "\t")
	for _, row := range rows.data {
		line := ""
		for _, data := range row.data {
			col := string(data)
			if data == nil {
				col = "NULL"
			}
			if len(line) > 0 {
				line = line + "\t"
			}
			line = line + col
		}
		res = res + "\n" + line
	}
	return res + "\n"
}

func dumpToByteRows(rows *sql.Rows) (*byteRows, error) {

	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	data := make([]byteRow, 0, 8)
	args := make([]interface{}, len(cols))
	for rows.Next() {
		tmp := make([][]byte, len(cols))
		for i := 0; i < len(args); i++ {
			args[i] = &tmp[i]
		}
		err := rows.Scan(args...)
		if err != nil {
			return nil, err
		}

		data = append(data, byteRow{tmp})
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return &byteRows{cols: cols, data: data, types: types}, nil
}

func compareByteRows(rs1, rs2 *byteRows) bool {
	n1 := len(rs1.data)
	n2 := len(rs2.data)
	if n1 != n2 {
		return false
	}
	for i := 0; i < n1; i++ {
		for j, c := range rs1.types {
			if rs1.data[i].data[j] == nil && rs2.data[i].data[j] == nil {
				continue
			}
			if rs1.data[i].data[j] == nil || rs2.data[i].data[j] == nil {
				return false
			}

			v1 := string(rs1.data[i].data[j])
			v2 := string(rs2.data[i].data[j])
			typeName := c.DatabaseTypeName()
			if typeName == "DECIMAL" ||
				typeName == "FLOAT" ||
				typeName == "DOUBLE" {
				f1, err := strconv.ParseFloat(v1, 10)
				if err != nil {
					panic(err)
				}
				f2, err := strconv.ParseFloat(v2, 10)
				if err != nil {
					panic(err)
				}
				if math.Abs(f1-f2) < 0.0001 || math.Abs((f1-f2)/(f1+f2)) < 0.001 {
					continue
				}
				return false
			} else {
				if v1 != v2 {
					return false
				}
			}
		}
	}
	return true
}

func init() {
	//rand.Seed(time.Now().UnixNano())
}
