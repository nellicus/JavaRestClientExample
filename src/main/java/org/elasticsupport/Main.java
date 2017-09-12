package org.elasticsupport;


import org.apache.commons.lang3.RandomStringUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Main {

    private static RestClient restClient = null;
    private static RestHighLevelClient restHighLevelClient = null;

    public static void main(String[] args) {

        initClient("localhost", 9200, "http");
        initHLClient(restClient);

        //index single doc
        indexDocument("testidx", "testtype", "testId", generateDoc(10, 20, 100));


        //bulk index
        bulkIndexDocument("testbulkidx2","testtype", 100, 10, 20,100);


        closeClient();
    }

    private static void initClient(String host, int port, String scheme) {

         restClient = RestClient.builder(
                new HttpHost(host, port, scheme
                )).build();

    }

    private static void initHLClient(RestClient restClient) {
        if (restClient != null) {
            restHighLevelClient =
                    new RestHighLevelClient(restClient);
        }
    }


    private static void closeClient() {
        if (restClient != null) {
            try {
                restClient.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static IndexResponse indexDocument(String indexName, String docType, String id, Map<String, Object> jsonMap) {
        /*
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap = new HashMap<>();
        jsonMap.put("user", "kimchy");
        jsonMap.put("postDate", new Date());
        jsonMap.put("message", "trying out Elasticsearch");
        */
        IndexResponse indexResponse = null;
        IndexRequest indexRequest = new IndexRequest(indexName, docType, id)
                .source(jsonMap);
        try {
            indexResponse = restHighLevelClient.index(indexRequest);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return indexResponse;

    }

    private static BulkResponse bulkIndexDocument(String indexName, String docType, int howManyDocs,int howManyFields, int fieldNamesLength, int fieldValuesLength){
        BulkRequest request = new BulkRequest();
        for (int i = 0; i <howManyDocs;i++){
            System.out.println(" Adding document " + i + " to the bulk request");
            request.add(new IndexRequest(indexName,docType,generateString(10)).source(generateDoc(howManyFields,fieldNamesLength,fieldValuesLength)));
        }

        BulkResponse bulkResponse = null;
        System.out.println("Sending bulk request with size in bytes[" +request.estimatedSizeInBytes()+"] kbytes["+((double)request.estimatedSizeInBytes()/1024)+"] mbytes[" + (double)request.estimatedSizeInBytes()/(1024*1024)+"]");
        try {
            bulkResponse = restHighLevelClient.bulk(request);
        } catch (Exception e){
            e.printStackTrace();
        }

        if (bulkResponse.hasFailures()) {
            for (BulkItemResponse bulkItemResponse : bulkResponse) {
                if (bulkItemResponse.isFailed()) {
                    BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                    System.out.println(failure.getMessage());
                }
            }
        }
        return bulkResponse;


    }


    private static ArrayList<Map<String, Object>> generateDocs(int howManyDocs, int howManyFields, int fieldNamesLength, int fieldValuesLength) {
        ArrayList<Map<String, Object>> docList = new ArrayList<>();
        for (int i = 0; i < howManyDocs; i++) {
            docList.add(generateDoc(howManyFields, fieldNamesLength, fieldValuesLength));
        }
        return docList;
    }

    private static Map<String, Object> generateDoc(int howManyFields, int fieldNamesLength, int fieldValuesLength) {
        Map<String, Object> documentMap = new HashMap<>();
        for (int i = 0; i < howManyFields; i++) {
            StringBuffer fieldName = new StringBuffer(generateString(fieldNamesLength));
            StringBuffer fieldValue = new StringBuffer(generateString(fieldValuesLength));
            documentMap.put(fieldName.toString(), fieldValue.toString());
        }
        return documentMap;
    }

    private static String generateString(int maxlength) {
        StringBuffer result = new StringBuffer();

        result.append(RandomStringUtils.randomAlphabetic(maxlength));

        return result.toString();
    }


}
