using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace sort
{
    public static class Sorter
    {
        private const int WORKER_COUNT = 8;
        private const int GB = 1024 * 1024 * 1024;

        private static void ClearTmpFiles()
        {
            Directory.GetFiles(".", "*.chunk.txt").ToList().ForEach(File.Delete);
            Directory.GetFiles(".", "*.merge.txt").ToList().ForEach(File.Delete);
        }

        private static string MergeSort(string left, string right, int parallelMerges)
        {
            var bufferSize = GB / 4 / (int)Math.Pow(2, parallelMerges);

            var resFileName = $"{Guid.NewGuid()}.merge.txt";
            using (var lfs = new FileStream(left, FileMode.Open, FileAccess.Read, FileShare.None, bufferSize, FileOptions.SequentialScan))
            using (var rfs = new FileStream(right, FileMode.Open, FileAccess.Read, FileShare.None, bufferSize, FileOptions.SequentialScan))
            using (var merge = File.Create(resFileName, bufferSize * 2))
            using (var lsr = new StreamReader(lfs))
            using (var rsr = new StreamReader(rfs))
            using (var msw = new StreamWriter(merge))
            {
                var leftSize = lfs.Length;
                var rightSize = rfs.Length;

                var leftLine = lsr.ReadLine().AsSpan();
                var rightLine = rsr.ReadLine().AsSpan();
                var leftReadedLines = 1;
                var rightReadedLines = 1;
                var outWritedLines = 0;

                while (!(leftLine.IsEmpty && rightLine.IsEmpty))
                {
                    var leftDelim = leftLine.IndexOf('.');
                    var rightDelim = rightLine.IndexOf('.');


                    var leftStr = leftLine.Slice(leftDelim + 1);
                    var rightStr = rightLine.Slice(rightDelim + 1);

                    var leftCompareToRight = LineComparer.CompareBySpans(leftStr, rightStr);
                    if (leftCompareToRight == 0)
                    {
                        var leftNumber = int.Parse(leftLine.Slice(0, leftDelim));
                        var rightNumber = int.Parse(rightLine.Slice(0, rightDelim));
                        if (leftNumber < rightNumber)
                            leftCompareToRight = -1;
                        else
                            leftCompareToRight = 1;
                    }

                    if ((leftCompareToRight < 0 && !leftLine.IsEmpty) || rightLine.IsEmpty)
                    {
                        msw.WriteLine(leftLine);
                        outWritedLines++;
                        leftLine = lsr.ReadLine().AsSpan();
                        if (!leftLine.IsEmpty)
                            leftReadedLines++;
                    }
                    else
                    {
                        msw.WriteLine(rightLine);
                        outWritedLines++;
                        rightLine = rsr.ReadLine().AsSpan();
                        if (!rightLine.IsEmpty)
                            rightReadedLines++;
                    }
                };

            }
            Console.WriteLine($"Merged {left} and {right} --->> {resFileName}");
            File.Delete(left);
            File.Delete(right);
            return resFileName;
        }

        private static void SortChunk(string inputFile, int chunkInd, long chunkStart, long nextChunkStart)
        {
            using var fs = new FileStream(inputFile, FileMode.Open, FileAccess.Read, FileShare.Read);
            using var charMemoryOwner = MemoryPool<char>.Shared.Rent((int)(nextChunkStart - chunkStart));

            fs.Seek(chunkStart, SeekOrigin.Begin);

            using (var memoryOwner = MemoryPool<byte>.Shared.Rent((int)(nextChunkStart - chunkStart)))
            {
                var memory = memoryOwner.Memory;
                fs.Read(memory.Span);
                Encoding.Default.GetChars(memory.Span, charMemoryOwner.Memory.Span);
            }

            var charBuffer = charMemoryOwner.Memory.Span;
            int cursor = 0;
            int delimPos = 0;
            int lastLFPos = -1;
            
            List<Line> lines;
            while (!_linesStore.TryPop(out lines)) ;
            
            Console.WriteLine($"{chunkInd}: start read buffer, size {charBuffer.Length / 1024 / 1024f}MB");            
            var date = DateTime.Now;

            int maxLength = 0;
            int linesCount = 0;
            while (cursor < charBuffer.Length)
            {
                if (charBuffer[cursor] == '.')
                    delimPos = cursor;
                if (charBuffer[cursor] == '\n')
                {
                    Line line = null;
                    if (lines.Count > linesCount)
                        line = lines[linesCount];
                    else
                    {
                        line = new Line();
                        lines.Add(line);
                    }

                    line.Number = int.Parse(charBuffer.Slice(lastLFPos + 1, delimPos - lastLFPos - 1));
                    line.DelimPos = delimPos;
                    line.OriginalStrStart = lastLFPos + 1;
                    line.OriginalStrEnd = cursor;

                    maxLength = cursor - lastLFPos > maxLength ? cursor - lastLFPos : maxLength;
                    lastLFPos = cursor;
                    linesCount++;
                }
                cursor++;

            }
            var readedLines = lines.Take(linesCount).ToList();
            readedLines.Sort(new LineComparer(charMemoryOwner.Memory));

            Console.WriteLine($"{chunkInd}: end read buffer, elapsed {DateTime.Now - date}");
            date = DateTime.Now;

            Span<byte> buffer = stackalloc byte[maxLength * 2];
            using (var outfs = File.Create($"{chunkInd}.chunk.txt", 1024 * 1024 * 8))
            {
                foreach (var line in readedLines)
                {
                    var length = Encoding.Default.GetBytes(charBuffer.Slice(line.OriginalStrStart, line.OriginalStrEnd - line.OriginalStrStart + 1), buffer);
                    outfs.Write(buffer.Slice(0, length));
                }
            }
            _linesStore.Push(lines);

            Console.WriteLine($"{chunkInd}: end write chunks, elapsed {DateTime.Now - date}");
        }

        private static ConcurrentStack<List<Line>> _linesStore;
        private static Dictionary<int, Dictionary<int, string>> _readyFiles;

        private static void Init()
        {
            _linesStore = new ConcurrentStack<List<Line>>();
            for (var i = 0; i < WORKER_COUNT; i++)
            {
                _linesStore.Push(new List<Line>());
            }

            _readyFiles = new Dictionary<int, Dictionary<int, string>>();
        }

        private static List<long> SplitToChunks(string file, out int mergeDepth, out long fileLength)
        {
            using var fs = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.Read);

            var memoryLimit = GB / 4;
            long workerMemory = memoryLimit / WORKER_COUNT;
            var chunkCount = 2;
            mergeDepth = 1;
            while (fs.Length / chunkCount > workerMemory)
            {
                chunkCount *= 2;
                mergeDepth++;
            }
            workerMemory = fs.Length / chunkCount;

            Console.WriteLine($"Worker memory: {workerMemory / 1024 / 1024f}MB. Chunks count {chunkCount}");

            var peekPos = 0L;
            Span<byte> buffer = stackalloc byte[100];
            Span<char> chars = stackalloc char[100];
            var chunkStarts = new List<long> { 0 };
            for (var i = 0; i < chunkCount; i++)
            {
                peekPos += workerMemory;
                fs.Seek(peekPos, SeekOrigin.Begin);
                var readedCount = fs.Read(buffer);

                Encoding.Default.GetChars(buffer, chars);
                var lf = chars.IndexOf('\n');

                if (peekPos + lf + 1 < fs.Length)
                    chunkStarts.Add(peekPos + lf + 1);

                if (chunkStarts.Count == chunkCount)
                    break;
            }

            fileLength = fs.Length;

            return chunkStarts;
        }

        private static void SortInternal(string inputFile, string outputFile)
        {
            var chunkStarts = SplitToChunks(inputFile, out int mergeDepth, out long fileLength);

            void TryMerge(int ind, string chunkName, int level = 0)
            {
                if (mergeDepth == level)
                {
                    File.Move(chunkName, outputFile, true);
                }

                string otherChunk;
                lock (_readyFiles)
                {
                    if (!_readyFiles.ContainsKey(level))
                        _readyFiles.Add(level, new Dictionary<int, string>());

                    var filesAtLevel = _readyFiles[level];
                    filesAtLevel.Add(ind, chunkName);
                    filesAtLevel.TryGetValue(ind % 2 == 0 ? ind + 1 : ind - 1, out otherChunk);
                }

                if (!string.IsNullOrEmpty(otherChunk))
                {
                    var mergeFile = MergeSort(chunkName, otherChunk, mergeDepth - level - 1);
                    TryMerge(ind / 2, mergeFile, level + 1);
                }
            }

            int processedChunkCount = 0;
            chunkStarts
                .Zip(chunkStarts.Skip(1).Union(new[] { fileLength }))
                .Select((c, ind) => new { ind, start = c.First, end = c.Second })
                .AsParallel()
                .WithDegreeOfParallelism(WORKER_COUNT)
                .ForAll(c =>
                {
                    SortChunk(inputFile, c.ind, c.start, c.end);
                    processedChunkCount++;
                    if (processedChunkCount == chunkStarts.Count) _linesStore = null;
                    TryMerge(c.ind, $"{c.ind}.chunk.txt");
                });
        }

        public static void Sort(string input, string output)
        {
            try
            {
                ClearTmpFiles();
                Init();
                SortInternal(input, output);
            }
            catch (Exception e)
            {
                ClearTmpFiles();
                Console.WriteLine($"Sort error: {e}");
            }
        }
    }
}
