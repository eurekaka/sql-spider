package main

import (
	"math/rand"
	"time"

	"github.com/zyguan/sql-spider/util"
	"github.com/zyguan/sql-spider/nodegen"
	"github.com/zyguan/sql-spider/exprgen"
)

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
	ts := getTableSchemas()
	emptyTrees := nodegen.GenerateNode(5)
	var trees []util.Tree
	for _, et := range emptyTrees {
		trees = append(trees, exprgen.GenExprTrees(et, ts, 3)...)
	}
	for _, t := range trees {
		util.SafePrint(t)
	}
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
