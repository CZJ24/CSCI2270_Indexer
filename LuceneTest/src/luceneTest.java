
import java.io.FileReader;
import java.io.IOException;
import java.io.File;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.ByteBuffersDirectory;

public class luceneTest {
    public static void main(String[] args) throws IOException, ParseException, org.apache.lucene.queryparser.classic.ParseException {
        StandardAnalyzer analyzer = new StandardAnalyzer();

        // 1. create the index
        Directory index = new ByteBuffersDirectory();

        IndexWriterConfig config = new IndexWriterConfig(analyzer);

        IndexWriter w = new IndexWriter(index, config);
        int count = 0;

        // 2. open the json file
        File dir = new File("./data");
        JSONParser parser = new JSONParser();
        long startTimeAddData = System.currentTimeMillis();
        for (File file : dir.listFiles()) {
            System.out.println(file.toString());
            JSONObject d = (JSONObject) parser.parse(new FileReader(file));
            JSONArray data = (JSONArray) d.get("dataset");

            for(Object o : data) {
                JSONObject logData = (JSONObject) o;
                String timeKey = String.valueOf(logData.get("timestamp"));
                String logValue = (String) logData.get("entry");
                //3. add log data to the indexer
                addDoc(w, timeKey, logValue);
                count = count+1;
            }
        }
        long endTimeAddData   = System.currentTimeMillis();
        NumberFormat formatter = new DecimalFormat("#0.00000");
        System.out.println("Execution time is " + formatter.format((endTimeAddData - startTimeAddData) / 1000d) + " seconds");
        System.out.println("Number of log: " + count);
        System.out.println("==========================================");
        w.close();

        //4. query string for searching
        String querystr1 = "1132524601";
        String querystr2 = "[1132524601 TO 1132524700]";
        String querystr3 = "1134528001";
        String querystr4 = "[1134528001 TO 1134528100]";
        String querystr5 = "1135532601";
        String querystr6 = "[1135532601 TO 1135532700]";
        searchTimeKey(analyzer,index,querystr1);
        searchTimeKey(analyzer,index,querystr2);
        searchTimeKey(analyzer,index,querystr3);
        searchTimeKey(analyzer,index,querystr4);
        searchTimeKey(analyzer,index,querystr5);
        searchTimeKey(analyzer,index,querystr6);


    }

    private static void addDoc(IndexWriter w, String timeKey, String logValue) throws IOException {
       // addDoc() uses for adding the data into the indexer
        Document doc = new Document();
        doc.add(new TextField("timeKey", timeKey, Field.Store.YES));

        // use a string field for isbn because we don't want it tokenized
        doc.add(new StringField("logValue", logValue, Field.Store.YES));
        w.addDocument(doc);
    }
    private static void searchTimeKey(StandardAnalyzer analyzer, Directory index,String querystr) throws org.apache.lucene.queryparser.classic.ParseException, IOException {
        // searchTimekey() uses for searching the given timestamp or time range within the indexer
        // and give print out the results

        System.out.println("Search for: " + querystr);

        long startTimeSearch = System.currentTimeMillis();
        Query q = new QueryParser("timeKey", analyzer).parse(querystr);

        int hitsPerPage = 1000000000;
        IndexReader reader = DirectoryReader.open(index);
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs docs = searcher.search(q, hitsPerPage);
        ScoreDoc[] hits = docs.scoreDocs;
        long endTimeSearch   = System.currentTimeMillis();
        NumberFormat formatter = new DecimalFormat("#0.00000");
        System.out.println("Execution time is " + formatter.format((endTimeSearch - startTimeSearch) / 1000d) + " seconds");
        // 4. display results
//        for(int i=0;i<hits.length;++i) {
//            int docId = hits[i].doc;
//            Document d = searcher.doc(docId);
//            System.out.println((i + 1) + ". " + d.get("timeKey") + "\t" + d.get("logValue"));
//        }
        System.out.println("Found " + hits.length + " hits.");

        System.out.println("==========================================");

        reader.close();
    }

}