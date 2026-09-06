using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace ParadoxReader.Sql
{
    /// <summary>
    /// Lexer for the constrained BDE Local SQL-like grammar supported by
    /// <see cref="SqlParser"/>. Single-quoted text is tokenized uniformly as
    /// <see cref="SqlTokenType.QuotedIdentifier"/> (covering both quoted
    /// table/column names, e.g. T.'FIELD', and string literal values, e.g.
    /// 'hello') - the parser disambiguates by grammar position, matching how
    /// BDE Local SQL itself overloads single-quoted syntax.
    /// </summary>
    internal sealed class SqlTokenizer
    {
        private static readonly HashSet<string> Keywords = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES", "UPDATE", "SET",
            "DELETE", "AND", "OR", "NOT", "NULL", "TRUE", "FALSE", "AS"
        };

        private readonly string text;
        private int pos;

        public SqlTokenizer(string text)
        {
            this.text = text ?? string.Empty;
            this.pos = 0;
        }

        public List<SqlToken> Tokenize()
        {
            var tokens = new List<SqlToken>();
            SqlToken tok;
            do
            {
                tok = NextToken();
                tokens.Add(tok);
            } while (tok.Type != SqlTokenType.EndOfInput);
            return tokens;
        }

        private char Current => pos < text.Length ? text[pos] : '\0';
        private char Peek(int ahead = 1) => (pos + ahead) < text.Length ? text[pos + ahead] : '\0';

        private SqlToken NextToken()
        {
            SkipWhitespace();

            int start = pos;
            if (pos >= text.Length)
                return new SqlToken { Type = SqlTokenType.EndOfInput, Text = string.Empty, Position = start };

            char c = Current;

            if (c == '\'')
                return ReadQuoted(start);

            if (c == '@')
                return ReadNamedParameter(start);

            if (c == '?')
            {
                pos++;
                return new SqlToken { Type = SqlTokenType.Parameter, Text = string.Empty, Position = start };
            }

            if (char.IsDigit(c) || (c == '.' && char.IsDigit(Peek())))
                return ReadNumber(start);

            if (char.IsLetter(c) || c == '_')
                return ReadIdentifierOrKeyword(start);

            switch (c)
            {
                case '.':
                    pos++;
                    return new SqlToken { Type = SqlTokenType.Dot, Text = ".", Position = start };
                case ',':
                    pos++;
                    return new SqlToken { Type = SqlTokenType.Comma, Text = ",", Position = start };
                case '(':
                    pos++;
                    return new SqlToken { Type = SqlTokenType.LParen, Text = "(", Position = start };
                case ')':
                    pos++;
                    return new SqlToken { Type = SqlTokenType.RParen, Text = ")", Position = start };
                case '*':
                    pos++;
                    return new SqlToken { Type = SqlTokenType.Star, Text = "*", Position = start };
                case ';':
                    pos++;
                    return new SqlToken { Type = SqlTokenType.Semicolon, Text = ";", Position = start };
                case '=':
                    pos++;
                    return new SqlToken { Type = SqlTokenType.Operator, Text = "=", Position = start };
                case '<':
                    pos++;
                    if (Current == '=') { pos++; return new SqlToken { Type = SqlTokenType.Operator, Text = "<=", Position = start }; }
                    if (Current == '>') { pos++; return new SqlToken { Type = SqlTokenType.Operator, Text = "<>", Position = start }; }
                    return new SqlToken { Type = SqlTokenType.Operator, Text = "<", Position = start };
                case '>':
                    pos++;
                    if (Current == '=') { pos++; return new SqlToken { Type = SqlTokenType.Operator, Text = ">=", Position = start }; }
                    return new SqlToken { Type = SqlTokenType.Operator, Text = ">", Position = start };
                case '!':
                    pos++;
                    if (Current == '=') { pos++; return new SqlToken { Type = SqlTokenType.Operator, Text = "<>", Position = start }; }
                    throw new SqlParseException($"Unexpected character '!' at position {start}");
                default:
                    throw new SqlParseException($"Unexpected character '{c}' at position {start}");
            }
        }

        private void SkipWhitespace()
        {
            while (pos < text.Length)
            {
                if (char.IsWhiteSpace(text[pos])) { pos++; continue; }
                // Support -- line comments for convenience.
                if (text[pos] == '-' && Peek() == '-')
                {
                    while (pos < text.Length && text[pos] != '\n') pos++;
                    continue;
                }
                break;
            }
        }

        private SqlToken ReadQuoted(int start)
        {
            pos++; // consume opening quote
            var sb = new StringBuilder();
            while (true)
            {
                if (pos >= text.Length)
                    throw new SqlParseException($"Unterminated quoted literal starting at position {start}");

                char c = text[pos];
                if (c == '\'')
                {
                    // '' is an escaped single quote inside the literal
                    if (Peek() == '\'')
                    {
                        sb.Append('\'');
                        pos += 2;
                        continue;
                    }
                    pos++; // consume closing quote
                    break;
                }
                sb.Append(c);
                pos++;
            }
            return new SqlToken { Type = SqlTokenType.QuotedIdentifier, Text = sb.ToString(), Position = start };
        }

        private SqlToken ReadNumber(int start)
        {
            int begin = pos;
            while (char.IsDigit(Current)) pos++;
            if (Current == '.' && char.IsDigit(Peek()))
            {
                pos++;
                while (char.IsDigit(Current)) pos++;
            }
            if (Current == 'e' || Current == 'E')
            {
                int savedPos = pos;
                pos++;
                if (Current == '+' || Current == '-') pos++;
                if (char.IsDigit(Current))
                {
                    while (char.IsDigit(Current)) pos++;
                }
                else
                {
                    pos = savedPos; // not actually an exponent
                }
            }
            string numText = text.Substring(begin, pos - begin);
            return new SqlToken { Type = SqlTokenType.NumberLiteral, Text = numText, Position = start };
        }

        private SqlToken ReadIdentifierOrKeyword(int start)
        {
            int begin = pos;
            while (char.IsLetterOrDigit(Current) || Current == '_') pos++;
            string idText = text.Substring(begin, pos - begin);
            var type = Keywords.Contains(idText) ? SqlTokenType.Keyword : SqlTokenType.Identifier;
            return new SqlToken { Type = type, Text = idText, Position = start };
        }

        /// <summary>
        /// Reads an ADO.NET-style named parameter placeholder, e.g. "@id" or
        /// "@SECVAL", used anywhere a value literal is expected in the grammar
        /// (see SqlParser.ParseValue). The token's Text is the parameter name
        /// without the leading '@'.
        /// </summary>
        private SqlToken ReadNamedParameter(int start)
        {
            pos++; // consume '@'
            int begin = pos;
            while (char.IsLetterOrDigit(Current) || Current == '_') pos++;
            if (pos == begin)
                throw new SqlParseException($"Expected parameter name after '@' at position {start}");
            string name = text.Substring(begin, pos - begin);
            return new SqlToken { Type = SqlTokenType.Parameter, Text = name, Position = start };
        }
    }

    public class SqlParseException : Exception
    {
        public SqlParseException(string message) : base(message) { }
    }
}
