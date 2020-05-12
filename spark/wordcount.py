import sys

from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext("local", "WordCount")

    input_path = str(sys.argv[0])
    output_path = str(sys.argv[1]) if len(sys.argv) > 1 else "./output.txt"

    input_file = sc.textFile(input_path)
    words = input_file.flatMap(lambda w: w.split(" "))
    ones = words.map(lambda word: (word, 1))
    reduced = ones.reduceByKey(lambda x , y : x + y)
    reduced.saveAsTextFile(output_path)

    sc.stop()