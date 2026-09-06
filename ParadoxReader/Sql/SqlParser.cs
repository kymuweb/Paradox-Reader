using System;
using System.Collections.Generic;
using System.Globalization;

namespace ParadoxReader.Sql
{
    /// <summary>
    /// Recursive-descent parser for a constrained BDE Local SQL-like subset:
    /// single-table SELECT/INSERT/UPDATE/DELETE with a WHERE clause built
    /// from =,&lt;&gt;,&lt;,&lt;=,&gt;,&gt;= comparisons combined with AND/OR
    /// and parentheses. No joins, subqueries, aggregates, GROUP BY, or
    /// ORDER BY are supported (see ParadoxSqlExecutor remarks).
    ///
    /// Grammar (informal EBNF):
    ///   statement    := selectStmt | insertStmt | updateStmt | deleteStmt
    ///   selectStmt   := 'SELECT' ('*' | columnList) 'FROM' tableRef ('WHERE' whereExpr)?
    ///   insertStmt   := 'INSERT' 'INTO' tableRef ('(' columnList ')')? 'VALUES' '(' valueList ')'
    ///   updateStmt   := 'UPDATE' tableRef ('AS'? alias)? 'SET' assignment (',' assignment)* ('WHERE' whereExpr)?
    ///   deleteStmt   := 'DELETE' 'FROM' tableRef ('WHERE' whereExpr)?
    ///   tableRef     := (quotedLiteral | identifier) ('AS'? identifier)?
    ///   columnList   := columnRef (',' columnRef)*
    ///   columnRef    := (identifier '.')? (identifier | quotedLiteral)
    ///   assignment   := columnRef '=' value
    ///   whereExpr    := orExpr
    ///   orExpr       := andExpr ('OR' andExpr)*
    ///   andExpr      := comparison ('AND' comparison)*
    ///   comparison   := '(' whereExpr ')' | columnRef operator value
    ///   value        := quotedLiteral | number | 'TRUE' | 'FALSE' | 'NULL'
    /// </summary>
    internal sealed class SqlParser
    {
        private readonly List<SqlToken> tokens;
        private int pos;
        private int positionalParameterCount; // sequential index used to synthesize names for '?' placeholders

        public SqlParser(string sql)
        {
            tokens = new SqlTokenizer(sql).Tokenize();
            pos = 0;
        }

        private SqlToken Current => tokens[pos];

        private bool IsKeyword(string kw) =>
            Current.Type == SqlTokenType.Keyword && string.Equals(Current.Text, kw, StringComparison.OrdinalIgnoreCase);

        private SqlToken Expect(SqlTokenType type, string what)
        {
            if (Current.Type != type)
                throw new SqlParseException($"Expected {what} at position {Current.Position}, found '{Current.Text}' ({Current.Type})");
            var t = Current;
            pos++;
            return t;
        }

        private SqlToken ExpectKeyword(string kw)
        {
            if (!IsKeyword(kw))
                throw new SqlParseException($"Expected keyword '{kw}' at position {Current.Position}, found '{Current.Text}'");
            var t = Current;
            pos++;
            return t;
        }

        public SqlStatement Parse()
        {
            SqlStatement stmt;
            if (IsKeyword("SELECT")) stmt = ParseSelect();
            else if (IsKeyword("INSERT")) stmt = ParseInsert();
            else if (IsKeyword("UPDATE")) stmt = ParseUpdate();
            else if (IsKeyword("DELETE")) stmt = ParseDelete();
            else throw new SqlParseException($"Expected SELECT/INSERT/UPDATE/DELETE at position {Current.Position}, found '{Current.Text}'");

            // Allow an optional trailing semicolon.
            if (Current.Type == SqlTokenType.Semicolon) pos++;

            if (Current.Type != SqlTokenType.EndOfInput)
                throw new SqlParseException($"Unexpected trailing input at position {Current.Position}: '{Current.Text}'");

            return stmt;
        }

        // ----------------------------------------------------------------
        // SELECT
        // ----------------------------------------------------------------

        private SelectStatement ParseSelect()
        {
            ExpectKeyword("SELECT");

            var stmt = new SelectStatement();
            if (Current.Type == SqlTokenType.Star)
            {
                pos++;
                stmt.IsSelectStar = true;
            }
            else
            {
                stmt.Columns = ParseColumnList();
            }

            ExpectKeyword("FROM");
            stmt.Table = ParseTableRef();

            if (IsKeyword("WHERE"))
            {
                pos++;
                stmt.Where = ParseWhereExpr();
            }

            return stmt;
        }

        // ----------------------------------------------------------------
        // INSERT
        // ----------------------------------------------------------------

        private InsertStatement ParseInsert()
        {
            ExpectKeyword("INSERT");
            ExpectKeyword("INTO");

            var stmt = new InsertStatement();
            stmt.Table = ParseTableRef();

            if (Current.Type == SqlTokenType.LParen)
            {
                pos++;
                stmt.Columns = ParseColumnList();
                Expect(SqlTokenType.RParen, "')'");
            }

            ExpectKeyword("VALUES");
            Expect(SqlTokenType.LParen, "'('");
            stmt.Values = ParseValueList();
            Expect(SqlTokenType.RParen, "')'");

            return stmt;
        }

        private List<SqlValue> ParseValueList()
        {
            var list = new List<SqlValue> { ParseValue() };
            while (Current.Type == SqlTokenType.Comma)
            {
                pos++;
                list.Add(ParseValue());
            }
            return list;
        }

        // ----------------------------------------------------------------
        // UPDATE
        // ----------------------------------------------------------------

        private UpdateStatement ParseUpdate()
        {
            ExpectKeyword("UPDATE");

            var stmt = new UpdateStatement();
            stmt.Table = ParseTableRef();

            ExpectKeyword("SET");
            stmt.Assignments = new List<SqlAssignment> { ParseAssignment() };
            while (Current.Type == SqlTokenType.Comma)
            {
                pos++;
                stmt.Assignments.Add(ParseAssignment());
            }

            if (IsKeyword("WHERE"))
            {
                pos++;
                stmt.Where = ParseWhereExpr();
            }

            return stmt;
        }

        private SqlAssignment ParseAssignment()
        {
            string col = ParseColumnRef();
            var opTok = Expect(SqlTokenType.Operator, "'='");
            if (opTok.Text != "=")
                throw new SqlParseException($"Expected '=' in SET clause at position {opTok.Position}, found '{opTok.Text}'");
            var value = ParseValue();
            return new SqlAssignment { ColumnName = col, Value = value };
        }

        // ----------------------------------------------------------------
        // DELETE
        // ----------------------------------------------------------------

        private DeleteStatement ParseDelete()
        {
            ExpectKeyword("DELETE");
            ExpectKeyword("FROM");

            var stmt = new DeleteStatement();
            stmt.Table = ParseTableRef();

            if (IsKeyword("WHERE"))
            {
                pos++;
                stmt.Where = ParseWhereExpr();
            }

            return stmt;
        }

        // ----------------------------------------------------------------
        // Shared: table ref, column list/ref, WHERE tree, values
        // ----------------------------------------------------------------

        private TableRef ParseTableRef()
        {
            string path;
            if (Current.Type == SqlTokenType.QuotedIdentifier)
            {
                path = Current.Text;
                pos++;
            }
            else if (Current.Type == SqlTokenType.Identifier)
            {
                path = Current.Text;
                pos++;
            }
            else
            {
                throw new SqlParseException($"Expected table name at position {Current.Position}, found '{Current.Text}'");
            }

            string alias = null;
            if (IsKeyword("AS"))
            {
                pos++;
                alias = Expect(SqlTokenType.Identifier, "alias").Text;
            }
            else if (Current.Type == SqlTokenType.Identifier)
            {
                // Bare alias, e.g. 'table.db' T -- but don't consume keywords like WHERE/SET.
                alias = Current.Text;
                pos++;
            }

            return new TableRef { Path = path, Alias = alias };
        }

        private List<string> ParseColumnList()
        {
            var list = new List<string> { ParseColumnRef() };
            while (Current.Type == SqlTokenType.Comma)
            {
                pos++;
                list.Add(ParseColumnRef());
            }
            return list;
        }

        /// <summary>
        /// Parses a column reference, tolerating an optional table-alias
        /// prefix (e.g. T.'FIELD' or T.FIELD) which is discarded since v1
        /// only supports single-table statements.
        /// </summary>
        private string ParseColumnRef()
        {
            string first;
            if (Current.Type == SqlTokenType.Identifier || Current.Type == SqlTokenType.QuotedIdentifier)
            {
                first = Current.Text;
                pos++;
            }
            else
            {
                throw new SqlParseException($"Expected column name at position {Current.Position}, found '{Current.Text}'");
            }

            if (Current.Type == SqlTokenType.Dot)
            {
                pos++;
                if (Current.Type == SqlTokenType.Identifier || Current.Type == SqlTokenType.QuotedIdentifier)
                {
                    var second = Current.Text;
                    pos++;
                    return second; // discard the alias prefix
                }
                throw new SqlParseException($"Expected column name after '.' at position {Current.Position}");
            }

            return first;
        }

        private WhereExpr ParseWhereExpr() => ParseOr();

        private WhereExpr ParseOr()
        {
            var left = ParseAnd();
            while (IsKeyword("OR"))
            {
                pos++;
                var right = ParseAnd();
                left = new WhereOr { Left = left, Right = right };
            }
            return left;
        }

        private WhereExpr ParseAnd()
        {
            var left = ParseComparisonOrGroup();
            while (IsKeyword("AND"))
            {
                pos++;
                var right = ParseComparisonOrGroup();
                left = new WhereAnd { Left = left, Right = right };
            }
            return left;
        }

        private WhereExpr ParseComparisonOrGroup()
        {
            if (Current.Type == SqlTokenType.LParen)
            {
                pos++;
                var inner = ParseWhereExpr();
                Expect(SqlTokenType.RParen, "')'");
                return inner;
            }

            string col = ParseColumnRef();
            var opTok = Expect(SqlTokenType.Operator, "comparison operator");
            var op = MapOperator(opTok.Text);
            var value = ParseValue();

            return new WhereComparison { ColumnName = col, Operator = op, Value = value };
        }

        private ParadoxCompareOperator MapOperator(string opText)
        {
            switch (opText)
            {
                case "=": return ParadoxCompareOperator.Equal;
                case "<>": return ParadoxCompareOperator.NotEqual;
                case "<": return ParadoxCompareOperator.Less;
                case "<=": return ParadoxCompareOperator.LessOrEqual;
                case ">": return ParadoxCompareOperator.Greater;
                case ">=": return ParadoxCompareOperator.GreaterOrEqual;
                default: throw new SqlParseException($"Unsupported operator '{opText}'");
            }
        }

        private SqlValue ParseValue()
        {
            if (Current.Type == SqlTokenType.QuotedIdentifier)
            {
                var s = Current.Text;
                pos++;
                return SqlValue.OfString(s);
            }
            if (Current.Type == SqlTokenType.NumberLiteral)
            {
                var s = Current.Text;
                pos++;
                return SqlValue.OfNumber(double.Parse(s, CultureInfo.InvariantCulture));
            }
            if (Current.Type == SqlTokenType.Parameter)
            {
                string name = Current.Text;
                pos++;
                if (string.IsNullOrEmpty(name))
                    name = "?" + positionalParameterCount++; // synthetic name for positional '?' placeholders
                return SqlValue.OfParameter(name);
            }
            if (IsKeyword("TRUE")) { pos++; return SqlValue.OfBool(true); }
            if (IsKeyword("FALSE")) { pos++; return SqlValue.OfBool(false); }
            if (IsKeyword("NULL")) { pos++; return SqlValue.OfNull(); }

            throw new SqlParseException($"Expected value literal at position {Current.Position}, found '{Current.Text}'");
        }
    }
}
