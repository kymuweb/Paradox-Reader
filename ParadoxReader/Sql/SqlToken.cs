using System;

namespace ParadoxReader.Sql
{
    internal enum SqlTokenType
    {
        Identifier,
        QuotedIdentifier,   // 'Foo' - Paradox uses single-quoted identifiers for table/column names
        StringLiteral,      // for BDE Local SQL, string literals are also single-quoted; we disambiguate
                            // by parser context rather than by tokenizer since both use the same quote char
        NumberLiteral,
        Keyword,
        Dot,
        Comma,
        LParen,
        RParen,
        Operator,           // = <> < <= > >=
        Star,
        Semicolon,
        Parameter,          // @name or positional ?
        EndOfInput
    }

    internal struct SqlToken
    {
        public SqlTokenType Type;
        public string Text;
        public int Position;

        public override string ToString() => $"{Type}:{Text}";
    }
}
