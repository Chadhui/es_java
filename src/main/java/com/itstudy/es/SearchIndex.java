package com.itstudy.es;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Iterator;
import java.util.Map;

public class SearchIndex {
    private TransportClient client;
    @Before
    public void init() throws Exception {
        //1.创建一个Settings对象，相当于一个配置信息，抓哟配置集群的名称
        Settings settings = Settings.builder()
                .put("cluster.name","my-elasticsearch")
                .build();
        //2.创建一个客户端client对象
        client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9302))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9303));
    }
    private void search(QueryBuilder queryBuilder) throws Exception{
        //执行查询
        SearchResponse searchResponse = client.prepareSearch("index_hello")
                .setTypes("article")
                //设置分页信息
                .setFrom(0)
                //设置分页显示的行数
                .setSize(5)
                .setQuery(queryBuilder)
                .get();
        //取查询结果
        SearchHits searchHits = searchResponse.getHits();
        //取查询结果的总记录数
        System.out.println("查询结果总记录数："+searchHits.getTotalHits());
        //查询结果列表
        Iterator<SearchHit> iterator = searchHits.iterator();
        while (iterator.hasNext()){
            SearchHit searchHit = iterator.next();
            //打印文档，以json格式输出
            System.out.println(searchHit.getSourceAsString());
            //取文档的属性
            System.out.println("--------文档属性--------");
            Map<String, Object> document = searchHit.getSource();
            System.out.println(document.get("id"));
            System.out.println(document.get("title"));
        }
    }

    /**
     * 根据id值进行查询
     * @throws Exception
     */
    @Test
    public void testSearchById() throws Exception{
        //创建一个查询对象
        QueryBuilder queryBuilder = QueryBuilders.idsQuery().addIds("4", "5");
        search(queryBuilder);
    }
    @Test
    public void testQueryStringQuery() throws Exception{
        //创建一个QueryBuilder对象
        QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("美国")
                .defaultField("title");
        //执行查询
        search(queryBuilder,"title");
    }
    /**
     * 根据关键词进行查询
     */
    @Test
    public void testQueryTerm() throws Exception{
        //创建一个QueryBuilder对象
        QueryBuilder queryBuilder = QueryBuilders.termQuery("title","世界");
        search(queryBuilder);
    }

    /**
     * 高亮显示的字段
     */
    private void search(QueryBuilder queryBuilder,String highlightField) throws Exception{
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        //显示高亮的字段
        highlightBuilder.field(highlightField);
        highlightBuilder.preTags("<em>");
        highlightBuilder.postTags("</em>");
        //执行查询
        SearchResponse searchResponse = client.prepareSearch("index_hello")
                .setTypes("article")
                .setQuery(queryBuilder)
                //设置分页信息
                .setFrom(0)
                //设置分页显示的行数
                .setSize(5)

                //设置高亮的字段
                .highlighter(highlightBuilder)
                .get();
        //取查询结果
        SearchHits searchHits = searchResponse.getHits();
        //取查询结果的总记录数
        System.out.println("查询结果总记录数："+searchHits.getTotalHits());
        //查询结果列表
        Iterator<SearchHit> iterator = searchHits.iterator();
        while (iterator.hasNext()){
            SearchHit searchHit = iterator.next();
            //打印文档，以json格式输出
            System.out.println(searchHit.getSourceAsString());
            //取文档的属性
            System.out.println("--------文档属性--------");
            Map<String, Object> document = searchHit.getSource();
            System.out.println(document.get("id"));
            System.out.println(document.get("title"));
            System.out.println("--------高亮字段显示--------");
            Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
            System.out.println(highlightFields);
            //取title高亮显示的结果
            HighlightField field = highlightFields.get(highlightField);
            Text[] fragments = field.getFragments();
            if(fragments!=null){
                String title = fragments[0].toString();
                System.out.println(title);
            }
        }
        client.close();
    }

}
