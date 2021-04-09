using System;
using static sort.Sorter;

const string INPUT_FILE = "out.txt";
const string OUTPUT_FILE = "res.txt";

var start = DateTime.Now;

Sort(INPUT_FILE, OUTPUT_FILE);

Console.WriteLine($"Split end in {DateTime.Now - start}");

Console.ReadKey();