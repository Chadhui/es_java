package com.itstudy.es;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;

public class ElasticSearchClientTest {
    TransportClient client;
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
    /**
     * 创建一个索引库
     * @throws Exception
     */
    @Test
    public void createIndex() throws Exception{
        //1.创建一个Settings对象，相当于一个配置信息，主要配置集群的名称
        Settings settings = Settings.builder()
                .put("cluster.name","my-elasticsearch")
                .build();
        //2.创建一个客户端Client对象
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9301));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9302));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9303));
        //3.使用client对象创建一个集群库
        client.admin().indices().prepareCreate("index_hello")
                //执行操作
                .get();
        //4.关闭client对象
        client.close();
    }
    /**
     * 2、使用Java客户端设置Mappings
     * 	步骤：
     * 	1）创建一个Settings对象
     * 	2）创建一个Client对象
     * 	3）创建一个mapping信息，应该是一个json数据，可以是字符串，也可以是XContextBuilder对象
     * 	4）使用client向es服务器发送mapping信息
     * 	5）关闭client对象
     */
    @Test
    public void setMappings() throws Exception{
        //1.创建一个Settings对象
        Settings settings = Settings
                .builder().put("cluster.name","my-elasticsearch").build();
        //2.创建一个Client对象
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9301))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9302))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9303));

        //3.创建一个Mapping信息，应该是一个json数据，可以是字符串，也可以是XContextBuilder对象
        /*{
            "article":{
            "properties":{
                "id":{
                    "type":"long",
                            "store":true
                },
                "title":{
                    "type":"text",
                            "store":true,
                            "index":true,
                            "analyzer":"ik_smart"
                },
                "content":{
                    "type":"text",
                            "store":true,
                            "index":true,
                            "analyzer":"ik_smart"
                }
            }
        }
        }*/
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("article")
                        .startObject("properties")
                            .startObject("id")
                                .field("type", "long")
                                .field("store", true)
                            .endObject()
                             .startObject("title")
                                .field("type", "text")
                                .field("store", true)
                                .field("analyzer", "ik_smart")
                            .endObject()
                            .startObject("content")
                                .field("type", "text")
                                .field("store", true)
                                .field("analyzer", "ik_smart")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();
        //4.使用client向es服务器发送mapping信息
        client.admin().indices()
                //设置要做映射的索引
                .preparePutMapping("index_hello")
                //设置要做映射的type
                .setType("article")
                //mapping信息，可以是XContentBuilder对象可以是json对象
                .setSource(builder)
                //执行操作
                .get();
        //5.关闭client对象
        client.close();
    }
    /**
     * 创建文档
     */
    @Test
    public void testAddDocument() throws Exception{
        //创建一个client对象
        //创建一个文档
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                    .field("id",1l)
                    .field("title","北方入秋速度明显加快 多地降温幅度最多可达10度")
                    .field("content","阿联酋一架客机在纽约机场被隔离 10名乘客病倒")
                .endObject();
        //把文档对象添加到索引库
        client.prepareIndex()
                //设置索引名称
                .setIndex("index_hello")
                //设置type
                .setType("article")
                //设置文档的id,如果不设置将自动生成一个id
                .setId("1")
                //设置文档的信息
                .setSource(builder)
                //执行操作
                .get();
        //关闭客户端
        client.close();
    }

    /**
     * 增加文档的第二种方法
     * @throws Exception
     */
    @Test
    public void testAddDocument2() throws Exception{
        //创建一个Article对象
        Article article = new Article();
        //设置对象属性
        article.setId(2l);
        article.setTitle("MH370坠毁在柬埔寨密林?中国一公司调十颗卫星去拍摄");
        article.setContent("警惕荒唐的死亡游戏!俄15岁少年输掉游戏后用电锯自杀");
        //把article对象转换为json字符串
        ObjectMapper objectMapper = new ObjectMapper();
        String jsonDocument = objectMapper.writeValueAsString(article);
        System.out.println(jsonDocument);
        //使用client对象把文档写入索引库
        client.prepareIndex("index_hello","article","3")
                .setSource(jsonDocument, XContentType.JSON)
                .get();
        //关闭客户端
        client.close();
    }
    /**
     * 批量增加文档
     */
    @Test
    public void testAddDocument3() throws Exception {
        for (int i = 5; i < 100; i++) {
            //创建一个Article对象
            Article article = new Article();
            //设置对象属性
            article.setId(i);
            article.setTitle("“零号病人”能找到吗？美国新冠疑云，世界需要答案"+i);
            article.setContent("德国病毒学家：新冠病毒人造论“纯属一派胡言"+i);
            //把article对象转换为json字符串
            ObjectMapper objectMapper = new ObjectMapper();
            String jsonDocument = objectMapper.writeValueAsString(article);
            System.out.println(jsonDocument);
            //使用client对象把文档写入索引库
            client.prepareIndex("index_hello", "article", i+"")
                    .setSource(jsonDocument, XContentType.JSON)
                    .get();

        }
        //关闭客户端
        client.close();
    }
}
