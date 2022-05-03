Set up Enviroment

1. Please download Java and related components from https://www.java.com/en/

2. Download Lucene from https://lucene.apache.org/core/downloads.html and add it as source file. If you have questions, please read this tutorial https://www.tutorialspoint.com/lucene/lucene_environment.htm

3. Download jar files and add it as dependencies, https://jar-download.com/artifacts/org.apache.lucene

Set up Data

1. Download log json files into the ./data directory
2. Change the path in Main() on Line 45 to the correct path to the data.

How to Run the Program
1. You can set the timestamp or time range you want to search as the querystr in the main() Line 68

2. Run the LuceneTest.java file, you will get result.