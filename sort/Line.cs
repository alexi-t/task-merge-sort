using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace sort
{
    public class Line
    {
        public int Number { get; set; }
        public int DelimPos { get; set; }
        public int OriginalStrStart { get; set; }
        public int OriginalStrEnd { get; set; }
    }

    public class LineComparer : IComparer<Line>
    {
        private readonly Memory<char> _memory;
        public LineComparer(Memory<char> memory)
        {
            _memory = memory;
        }

        public static int CompareBySpans(ReadOnlySpan<char> left, ReadOnlySpan<char> right)
        {
            var max = Math.Max(left.Length, right.Length);
            for(var i = 0; i< max;i++)
            {
                if (i == left.Length && i == right.Length)
                    return 0;
                if (i == left.Length)
                    return -1;
                if (i == right.Length)
                    return 1;

                var l = left[i];
                var r = right[i];

                var compare = l.CompareTo(r);
                if (compare != 0)
                    return compare;
            }
            return 0;
        }

        public int Compare(Line x, Line y)
        {
            var strCompareResult = CompareBySpans(
                _memory.Slice(x.DelimPos +1, x.OriginalStrEnd - x.DelimPos - 2).Span,
                _memory.Slice(y.DelimPos +1, y.OriginalStrEnd - y.DelimPos - 2).Span);
            if (strCompareResult == 0)
                return x.Number - y.Number;
            else
                return strCompareResult;
        }
    }
}
