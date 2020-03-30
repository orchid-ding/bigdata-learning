package com.travel.datasource;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.support.http.StatViewServlet;
import com.alibaba.druid.support.http.WebStatFilter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Configuration
public class DruidConfig {

    /**
     * 添加 DruidDataSource 组件到容器中，并绑定属性：
     * 将自定义的 Druid 数据源添加到容器中，不再让 Spring Boot 自动创建
     * 这样做的目的是：绑定全局配置文件中的 druid 数据源属性到 com.alibaba.druid.pool.DruidDataSource
     * 从而让它们生效
     * @ConfigurationProperties(prefix = "spring.datasource")：作用就是将 全局配置文件中 前缀为 spring.datasource
     * 的属性值注入到 com.alibaba.druid.pool.DruidDataSource 的同名参数中
     */
    @ConfigurationProperties(prefix = "spring.datasource")
    @Bean
    public DataSource druidDataSource() {
        return new DruidDataSource(){
            @Override
            public void configFromPropety(Properties properties) {
                properties.setProperty("zookeeper.znode.parent", "/HBase");
                super.configFromPropety(properties);
            }
        };
    }

    /**
     * 注册 Druid 监控之管理后台的 Servlet
     */
    @Bean
    public ServletRegistrationBean servletRegistrationBean() {
        ServletRegistrationBean bean = new ServletRegistrationBean(
                new StatViewServlet(), "/druid/*");

        /**
         * 这些参数可以在 com.alibaba.druid.support.http.StatViewServlet 的父类 com.alibaba.druid.support.http.ResourceServlet 中找到
         * loginUsername：Druid 后台管理界面的登录账号
         * loginPassword：Druid 后台管理界面的登录密码
         * allow：Druid 白名单，后台允许谁可以访问,多个用逗号分割， 如果allow没有配置或者为空，则允许所有访问
         *      initParams.put("allow", "localhost")：表示只有本机可以访问
         *      initParams.put("allow", "")：为空或者为null时，表示允许所有访问
         * deny：Druid 黑名单，后台拒绝谁访问，多个用逗号分割 (共同存在时，deny优先于allow)
         *      initParams.put("deny", "192.168.1.20");表示禁止此ip访问
         */
        Map<String, String> initParams = new HashMap<>();
//        initParams.put("loginUsername", "admin");
//        initParams.put("loginPassword", "123456");
//        initParams.put("allow", "");
        initParams.put("zookeeper.znode.parent", "/HBase");
        /*initParams.put("deny", "192.168.1.20");*/

        /** 设置初始化参数*/
        bean.setInitParameters(initParams);
        return bean;
    }

    /**
     * 配置 Druid 监控之 web 监控的 filter
     * 这个过滤器的作用就是统计 web 应用请求中所有的数据库信息，
     * 比如 发出的 sql 语句，sql 执行的时间、请求次数、请求的 url 地址、以及seesion 监控、数据库表的访问次数 等等。
     */
    @Bean
    public FilterRegistrationBean filterRegistrationBean() {
        FilterRegistrationBean bean = new FilterRegistrationBean();
        bean.setFilter(new WebStatFilter());

        /** exclusions：设置哪些请求进行过滤排除掉，从而不进行统计*/
        Map<String, String> initParams = new HashMap<>();
        initParams.put("exclusions", "*.js,*.gif,*.jpg,*,png,*.css,/druid/*");
        bean.setInitParameters(initParams);

        /** "/*" 表示过滤所有请求*/
        bean.setUrlPatterns(Arrays.asList("/*"));
        return bean;
    }

}