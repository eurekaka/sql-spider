package util

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type Type uint

type TypeMask uint

func (tm TypeMask) Contain(t Type) bool {
	if t == 0 {
		panic("??")
	}
	return uint(tm)&uint(t) > 0
}

func (tm TypeMask) Any() Type {
	for i := uint(0); i < 15; i++ {
		if tm.Contain(Type(1 << i)) {
			return 1 << i
		}
	}
	panic(fmt.Sprintf("DEBUG %v", tm))
}

func (tm TypeMask) Has(t TypeMask) bool {
	return tm&t > 0
}

func (tm TypeMask) All() []Type {
	ret := make([]Type, 0, 15)
	for i := uint(0); i < 15; i++ {
		if tm.Contain(Type(1 << i)) {
			ret = append(ret, Type(1<<i))
		}
	}
	if len(ret) == 0 {
		panic(fmt.Sprintf("DEBUG %v", tm))
	}
	return ret
}

const (
	ETInt Type = 1 << iota
	ETReal
	ETDecimal
	ETString
	ETDatetime
	ETTimestamp
	ETDuration
	ETJson
)

type Expr interface {
	ToSQL() string
	Children() []Expr
	Clone() Expr
	RetType() Type
}

type Func struct {
	Name     string
	retType  Type
	children []Expr
}

func NewFunc(name string, retType Type) *Func {
	return &Func{name, retType, nil}
}

func (f *Func) Children() []Expr {
	return f.children
}

func (f *Func) ToSQL() string {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("============================")
			fmt.Println("====>>>", f.Name, len(f.children))
			fmt.Println("============================")
			panic("??")
		}
	}()

	infixFn := func(op string) string {
		return fmt.Sprintf("(%s %s %s)", f.children[0].ToSQL(), op, f.children[1].ToSQL())
	}
	switch f.Name {
	case FuncEQ:
		return infixFn("=")
	case FuncNE:
		return infixFn("!=")
	case FuncGE:
		return infixFn(">=")
	case FuncGT:
		return infixFn(">")
	case FuncLE:
		return infixFn("<=")
	case FuncLT:
		return infixFn("<")
	case FuncLogicOr:
		return infixFn("OR")
	case FuncLogicAnd:
		return infixFn("AND")
	case FuncAnd:
		return infixFn("&")
	case FuncOr:
		return infixFn("|")
	case FuncLogicXor, FuncXor:
		return infixFn("^")
	case FuncPlus:
		return infixFn("+")
	case FuncMinus:
		return infixFn("-")
	case FuncUnaryMinus:
		return "-(" + f.children[0].ToSQL() + ")"
	case FuncDiv:
		return infixFn("/")
	case FuncMul:
		return infixFn("*")
	case FuncMod:
		return infixFn("%")
	case FuncIntDiv:
		return infixFn("DIV")
	case FuncIsTrue:
		return "((" + f.children[0].ToSQL() + ") is true)"
	default:
		args := make([]string, len(f.children))
		for i, e := range f.children {
			args[i] = e.ToSQL()
		}
		return strings.ToUpper(f.Name) + "(" + strings.Join(args, ", ") + ")"
	}
}

func (f *Func) AppendArg(expr Expr) {
	f.children = append(f.children, expr)
}

func (f *Func) SetRetType(tm TypeMask) {
	f.retType = Type(tm)
}

func (f *Func) Clone() Expr {
	xs := make([]Expr, 0, len(f.children))
	for _, c := range f.children {
		xs = append(xs, c.Clone())
	}
	return &Func{
		Name:     f.Name,
		retType:  f.retType,
		children: xs,
	}
}

func (f *Func) RetType() Type {
	return f.retType
}

type Constant struct {
	val     string
	retType Type
}

func NewConstant(val string, retType Type) Constant {
	return Constant{val, retType}
}

func (c Constant) Children() []Expr {
	return nil
}

func (c Constant) ToSQL() string {
	return c.val
}

func (c Constant) ToString() string {
	if c.val[:1] == "'" {
		return c.val
	}
	return "'" + c.val + "'"
}

func (c Constant) Clone() Expr {
	return Constant{c.val, c.retType}
}

func (c Constant) RetType() Type {
	return c.retType
}

type Column struct {
	col     string
	retType Type
}

func NewColumn(col string, retType Type) Column {
	return Column{col, retType}
}

func (c Column) Children() []Expr {
	return nil
}

func (c Column) ToSQL() string {
	return c.col
}

func (c Column) Clone() Expr {
	return Column{c.col, c.retType}
}

func (c Column) RetType() Type {
	return c.retType
}

type Node interface {
	Columns() []Expr
	ToBeautySQL(level int) string
	ToExecuteStmts(cnt int) []string
	ToString() string
	Children() []Node
	ParamCnt() int
	SetParamCnt(cnt int)
	Clone() Node
	AddChild(node Node)
}

type Tree Node

type NodeType uint

const (
	NTJoin NodeType = 1 << iota
	NTAgg
	NTProjector
	NTFilter
	NTTable
	NTLimit
	NTOrderBy
)

type NodeTypeMask uint

func (m NodeTypeMask) Contain(tp NodeType) bool {
	return uint(m)&uint(tp) > 0
}

func (m NodeTypeMask) Add(tp NodeType) NodeTypeMask {
	m = NodeTypeMask(uint(m) | uint(tp))
	return m
}

func (m NodeTypeMask) Remove(tp NodeType) NodeTypeMask {
	m = NodeTypeMask(uint(m) ^ uint(tp))
	return m
}

type baseNode struct {
	children []Node
	paramCnt int
	stmtName string
}

func (b *baseNode) Children() []Node {
	return b.children
}

func (b *baseNode) ParamCnt() int {
	return b.paramCnt
}

func (b *baseNode) SetParamCnt(cnt int) {
	b.paramCnt = cnt
}

func (b *baseNode) clone() *baseNode {
	xs := make([]Node, 0, len(b.children))
	for _, c := range b.children {
		xs = append(xs, c.Clone())
	}
	return &baseNode{
		children: xs,
		paramCnt: b.paramCnt,
		stmtName: "",
	}
}

func (b *baseNode) AddChild(node Node) {
	b.children = append(b.children, node)
}

var stmtNo uint32

func (b *baseNode) addPrepare(level int, sql string) string {
	if level > 0 {
		return sql
	}
	no := atomic.AddUint32(&stmtNo, 1)
	b.stmtName = "stmt" + fmt.Sprintf("%v", no)
	return "prepare " + b.stmtName + " from \"" + sql + "\""
}

func (b *baseNode) ToExecuteStmts(cnt int) []string {
	stmts := make([]string, 2*cnt)
	for i := 0; i < 2*cnt; i = i+2 {
		setStr := "set "
		execStr := "execute " + b.stmtName + " using "
		for j := 0; j < b.paramCnt; j++ {
			val := GenConstant(TypeDefault).ToString()
			end := ","
			if j == b.paramCnt - 1 {
				end = ";"
			}
			param := "@p" + fmt.Sprintf("%v", j)
			setStr = setStr + param + "=" + val + end
			execStr = execStr + param + end
		}
		stmts[i] = setStr
		stmts[i+1] = execStr
	}
	return stmts
}

type Filter struct {
	baseNode
	Where Expr
}

func (f *Filter) Columns() []Expr {
	return f.children[0].Columns()
}

func (f *Filter) ToBeautySQL(level int) string {
	sql := "SELECT * FROM (\n" +
		f.children[0].ToBeautySQL(level+1) + ") t WHERE " + f.Where.ToSQL()
	return f.baseNode.addPrepare(level, sql)
}

func (f *Filter) Clone() Node {
	var where Expr
	if f.Where != nil {
		where = f.Where.Clone()
	}
	return &Filter{
		*f.baseNode.clone(),
		where,
	}
}

func (f *Filter) ToString() string {
	return "Filter(" + f.children[0].ToString() + ")"
}

type Projector struct {
	baseNode
	Projections []Expr
}

func NewProjector(p []Expr) *Projector {
	return &Projector{Projections: p}
}

func (p *Projector) Columns() []Expr {
	cols := make([]Expr, len(p.Projections))
	for i, e := range p.Projections {
		cols[i] = NewColumn("c"+strconv.Itoa(i), e.RetType())
	}
	return cols
}

func (p *Projector) ToBeautySQL(level int) string {
	cols := make([]string, len(p.Projections))
	for i, e := range p.Projections {
		cols[i] = e.ToSQL() + " AS c" + strconv.Itoa(i)
	}
	sql := strings.Repeat(" ", level) + "SELECT " + strings.Join(cols, ", ") + " FROM (\n" +
		p.children[0].ToBeautySQL(level+1) + "\n" +
		strings.Repeat(" ", level) + ") AS t"
	return p.baseNode.addPrepare(level, sql)
}

func (p *Projector) Clone() Node {
	ps := make([]Expr, 0, len(p.Projections))
	for _, x := range p.Projections {
		ps = append(ps, x.Clone())
	}
	return &Projector{
		*p.baseNode.clone(),
		ps,
	}
}

func (p *Projector) ToString() string {
	return "Projector(" + p.children[0].ToString() + ")"
}

type OrderBy struct {
	baseNode
	OrderByExprs []Expr
}

func NewOrderBy(OrderByExprs []Expr) *OrderBy {
	return &OrderBy{OrderByExprs: OrderByExprs}
}

func (o *OrderBy) Columns() []Expr {
	return o.children[0].Columns()
}

func (o *OrderBy) ToBeautySQL(level int) string {
	orderBy := make([]string, 0, len(o.OrderByExprs))
	for _, e := range o.OrderByExprs {
		orderBy = append(orderBy, e.ToSQL())
	}

	sql := "SELECT * FROM (" + o.children[0].ToBeautySQL(level+1) + ") t ORDER BY " + strings.Join(orderBy, ", ")
	return o.baseNode.addPrepare(level, sql)
}

func (o *OrderBy) Clone() Node {
	orderBy := make([]Expr, 0, len(o.OrderByExprs))
	for _, or := range o.OrderByExprs {
		orderBy = append(orderBy, or.Clone())
	}
	return &OrderBy{
		*o.baseNode.clone(),
		orderBy,
	}
}

func (o *OrderBy) ToString() string {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("=======")
			fmt.Printf("child:%d", len(o.children))
			fmt.Println("=======")
			panic("wocao order")
		}
	}()
	return "Order(" + o.children[0].ToString() + ")"
}

type Limit struct {
	baseNode
	Limit int
}

func (l *Limit) Columns() []Expr {
	return l.children[0].Columns()
}

func (l *Limit) ToBeautySQL(level int) string {
	sql := l.children[0].ToBeautySQL(level+1) + " LIMIT " + strconv.Itoa(l.Limit)
	return l.baseNode.addPrepare(level, sql)
}

func (l *Limit) Clone() Node {
	return &Limit{
		baseNode: *l.baseNode.clone(),
		Limit:    0,
	}
}

func (l *Limit) ToString() string {
	return "Limit(" + l.children[0].ToString() + ")"
}

type Agg struct {
	baseNode
	AggExprs     []Expr
	GroupByExprs []Expr
}

func (a *Agg) Columns() []Expr {
	ret := make([]Expr, 0, len(a.AggExprs)+len(a.GroupByExprs))
	ret = append(ret, a.GroupByExprs...)
	ret = append(ret, a.AggExprs...)
	return ret
}

func (a *Agg) ToBeautySQL(level int) string {
	ret := a.Columns()
	aggs := make([]string, 0, len(ret))
	for i, e := range ret {
		name := fmt.Sprintf("c%v", i)
		eSQL := e.ToSQL()
		if eSQL != name {
			eSQL += " AS " + name
		}
		aggs = append(aggs, eSQL)
	}
	groupBy := make([]string, 0, len(a.GroupByExprs))
	for _, e := range a.GroupByExprs {
		groupBy = append(groupBy, e.ToSQL())
	}
	groupBySQL := "GROUP BY " + strings.Join(groupBy, ", ")
	if len(groupBy) == 0 {
		groupBySQL = ""
	}
	sql := strings.Repeat(" ", level) + "SELECT " + strings.Join(aggs, ", ") + " FROM (\n" +
		a.children[0].ToBeautySQL(level+1) + "\n" +
		strings.Repeat(" ", level) + ") AS t " + groupBySQL
	return a.baseNode.addPrepare(level, sql)
}

func (a *Agg) Clone() Node {
	aggExpr := make([]Expr, 0, len(a.AggExprs))
	for _, agg := range a.AggExprs {
		aggExpr = append(aggExpr, agg.Clone())
	}
	groupBy := make([]Expr, 0, len(a.GroupByExprs))
	for _, gb := range a.GroupByExprs {
		groupBy = append(groupBy, gb.Clone())
	}

	return &Agg{
		*a.baseNode.clone(),
		aggExpr, groupBy,
	}
}

func (a *Agg) ToString() string {
	return "Agg(" + a.children[0].ToString() + ")"
}

type Join struct {
	baseNode
	JoinCond Expr
}

func (j *Join) Columns() []Expr {
	exprs := make([]Expr, 0, len(j.children[0].Columns())+len(j.children[1].Columns()))
	for _, expr := range j.children[0].Columns() {
		exprs = append(exprs, expr)
	}
	for _, expr := range j.children[1].Columns() {
		exprs = append(exprs, expr)
	}
	return exprs
}

func (j *Join) ToBeautySQL(level int) string {
	l, r := j.children[0], j.children[1]
	lLen, rLen := len(l.Columns()), len(r.Columns())
	cols := make([]string, lLen+rLen)
	for i := 0; i < lLen; i++ {
		cols[i] = "t1.c" + strconv.Itoa(i) + " AS " + "c" + strconv.Itoa(i)
	}
	for i := 0; i < rLen; i++ {
		cols[i+lLen] = "t2.c" + strconv.Itoa(i) + " AS " + "c" + strconv.Itoa(i+lLen)
	}
	sql := strings.Repeat(" ", level) + "SELECT " + strings.Join(cols, ",") + " FROM (\n" +
		l.ToBeautySQL(level+1) + ") AS t1, (\n" +
		r.ToBeautySQL(level+1) + ") AS t2\n" +
		strings.Repeat(" ", level) + " WHERE " + j.JoinCond.ToSQL()
	return j.baseNode.addPrepare(level, sql)
}

func (j *Join) Clone() Node {
	var cond Expr
	if j.JoinCond != nil {
		cond = j.JoinCond.Clone()
	}
	return &Join{
		*j.baseNode.clone(),
		cond,
	}
}

func (j *Join) ToString() string {
	return "Join(" + j.children[0].ToString() + "," + j.children[1].ToString() + ")"
}

type Table struct {
	baseNode
	Schema TableSchema

	SelectedColumns []int
}

func NewTable(Schema TableSchema, SelectedColumns []int) *Table {
	return &Table{Schema: Schema, SelectedColumns: SelectedColumns}
}

func (t *Table) Columns() []Expr {
	cols := make([]Expr, len(t.SelectedColumns))
	for i, idx := range t.SelectedColumns {
		cols[i] = NewColumn("c"+strconv.Itoa(i), t.Schema.Columns[idx].RetType())
	}
	return cols
}

func (t *Table) ToBeautySQL(level int) string {
	cols := make([]string, len(t.SelectedColumns))
	for i, idx := range t.SelectedColumns {
		cols[i] = t.Schema.Columns[idx].col + " AS c" + strconv.Itoa(i)
	}
	sql := strings.Repeat(" ", level) + "SELECT " + strings.Join(cols, ", ") + " FROM " + t.Schema.Name
	return t.baseNode.addPrepare(level, sql)
}

func (t *Table) Clone() Node {
	t1 := &Table{
		*t.baseNode.clone(),
		t.Schema,
		nil,
	}
	for _, s := range t.SelectedColumns {
		t1.SelectedColumns = append(t1.SelectedColumns, s)
	}
	return t1
}

func (t *Table) ToString() string {
	return "Table"
}

type TableSchema struct {
	Name    string
	Columns []Column
}

type TableSchemas []TableSchema

func GenConstant(tp TypeMask) Constant {
	if rand.Intn(100) <= 1 {
		return NewConstant("NULL", tp.Any())
	}

	var ct Type
	var cv string
	tps := tp.All()
	t := rand.Intn(len(tps))
	ct = tps[t]
	switch ct {
	case ETInt:
		cv = GenIntLiteral()
	case ETReal, ETDecimal:
		cv = GenRealLiteral()
	case ETString:
		cv = GenStringLiteral()
	case ETDatetime:
		cv = GenDateTimeLiteral()
	default:
		ct = tp.Any()
		cv = "NULL"
	}
	return NewConstant(cv, ct)
}

func GenParam(tp TypeMask) Constant {
	tps := tp.All()
	t := rand.Intn(len(tps))
	return NewConstant("?", tps[t])
}

func GenDateTimeLiteral() string {
	t := time.Unix(2000000000 + rand.Int63n(10), 30000000000 + rand.Int63n(5))
	return t.Format("'2006-01-02 15:04:05'")
}

func GenIntLiteral() string {
	return fmt.Sprintf("%d", rand.Int63n(50))
}

func GenRealLiteral() string {
	return fmt.Sprintf("%.2f", rand.Float64())
}

func GenStringLiteral() string {
	// n := rand.Intn(10) + 1
	buf := make([]byte, 0, 2)
	for i := 0; i < 2; i++ {
		x := rand.Intn(62)
		if x < 26 {
			buf = append(buf, byte('a'+x))
		} else if x < 52 {
			buf = append(buf, byte('A'+x-26))
		} else {
			buf = append(buf, byte('0'+x-52))
		}
	}
	return "'" + string(buf) + "'"
}

func GenExpr(cols []Expr, tp TypeMask, validate ValidateExprFn) (Expr, int) {
	var gen func(lv int, tp TypeMask, validate ValidateExprFn) (Expr, int)
	gen = func(lv int, tp TypeMask, validate ValidateExprFn) (Expr, int) {
		count := 10000
		for count > 0 {
			count--
			switch f := GenExprFromProbTable(tp, lv); f {
			case Col:
				cc := make([]Expr, 0, len(cols))
				for _, col := range cols {
					if tp.Contain(col.RetType()) {
						cc = append(cc, col)
					}
				}
				if len(cc) == 0 {
					continue
				}
				expr := cc[rand.Intn(len(cc))]
				if !validate(expr) {
					continue
				}
				return expr, 0
			case Const:
				expr := GenParam(tp)
				if !validate(expr) {
					continue
				}
				return expr, 1
			default:
				fnSpec := FuncInfos[f]
				n := fnSpec.MinArgs
				if fnSpec.MaxArgs > fnSpec.MinArgs {
					n = rand.Intn(fnSpec.MaxArgs-fnSpec.MinArgs) + fnSpec.MinArgs
				}
				expr := &Func{Name: f}
				expr.SetRetType(fnSpec.ReturnType)
				ok := true
				paramCnt := int(0)
				for i := 0; i < n; i++ {
					subExpr, cnt := gen(lv+1, fnSpec.ArgTypeMask(i, expr.Children()), RejectAllConstatns)
					if subExpr == nil {
						ok = false
						break
					}
					expr.AppendArg(subExpr)
					paramCnt += cnt
				}
				if !ok {
					continue
				}
				if lv == 0 && !validate(expr) {
					continue
				}
				if fnSpec.Validate != nil && !fnSpec.Validate(expr) {
					continue
				}
				return expr, paramCnt
			}
		}
		panic("???")
	}
	return gen(0, tp, validate)
}

func SafePrint(t Tree) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("============================")
			fmt.Println(t.ToString())
			fmt.Println("============================")
			panic("??")
		}
	}()
	fmt.Println(t.ToBeautySQL(0) + ";")
	execs := t.ToExecuteStmts(10)
	for _, e := range execs {
		fmt.Println(e)
	}
	fmt.Println()
}
