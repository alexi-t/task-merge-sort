using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

var words = File.ReadAllLines("words_alpha.txt"); //https://github.com/dwyl/english-words/blob/master/words_alpha.txt

string RandomWord(Random rnd) => words[rnd.Next(words.Length)];

string NewPhrase(Random rnd)
{
    var wordsCount = rnd.Next(1, 4);
    return string.Join(" ", Enumerable.Range(1, wordsCount).Select(wordPos => RandomWord(rnd)));
}

Console.Write("Enter limit in GBs: ");
var limit = int.Parse(Console.ReadLine());

var start = DateTime.Now;

const long GB = 1024 * 1024 * 1024;
const int sbSizeLimit = (int)GB / 32;
const int workers = 8;

long gbsDumped = 0;

object writeSync = new object();
long linesWritten = 0;
using (var sw = new StreamWriter(File.Create("out.txt")))
{
    Enumerable.Range(1, workers).AsParallel().ForAll((workerId) =>
    {
        var sb = new StringBuilder(sbSizeLimit, sbSizeLimit);
        var rnd = new Random(workerId);

        while (gbsDumped < GB * limit)
        {
            var newPhrase = NewPhrase(rnd);
            var newNumber = rnd.Next(int.MaxValue);

            if (sb.Length + 11 + newPhrase.Length + 2 > sbSizeLimit)
            {
                lock (writeSync)
                {
                    sw.Write(sb.ToString());
                    gbsDumped = sw.BaseStream.Position;
                }
                sb.Clear();
                Console.WriteLine($"Dump to disk\r Current size {(float)gbsDumped / GB}");
            }

            sb.Append(newNumber);
            sb.Append(".");
            sb.AppendLine(newPhrase);
            linesWritten++;
        }
    });
}

Console.WriteLine($"Time elapsed: {DateTime.Now - start}. Lines written {linesWritten}");
Console.ReadKey();