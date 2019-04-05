package assignment1.part2;

import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.BasicConfigurator;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


public class TwitterData{
        public static void main(String args[]) throws IOException {

            BasicConfigurator.configure();

            ConfigurationBuilder cb = new ConfigurationBuilder();
                    cb.setOAuthConsumerKey("nTCZtBrM5jsnlGAGNosLnQ4xQ");
                    cb.setOAuthConsumerSecret("brP8edGD5f55eJigfwRUXU5vLHphxF0biiqFqkFAxVSGZZ8qOD");
                    cb.setOAuthAccessToken("1092605088940679171-aqqusCyGVIu2lnhxEplaBLwJQUfJmX");
                    cb.setOAuthAccessTokenSecret("ZJWGvUysV66hTreh10JPWWCiZImzBvC8mc09eWzKktTUS");

            Twitter twitter = new TwitterFactory(cb.build()).getInstance();

            //FileWriter fw;
           // fw = new FileWriter("india.txt");

            Query query = new Query("Trump");
            //query.setSince("2019-01-25");
            //query.setUntil("2019-02-06");
            query.setLang("en");
            query.setCount(100);
            QueryResult result;

            List<Status> tweetList = new ArrayList <Status>();
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

            String dst = args[0];
            //+ "/Assignment1/Part2";

            Configuration con = new Configuration();
            con.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/core-site.xml"));
            con.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/hdfs-site.xml"));
            FileSystem fs = FileSystem.get(URI.create(dst), con);
            //con.set("fs.defaultFS","hdfs://cshadoop1");

            try {
                for(int days=6; days>0; days--){
                    query.setUntil(format.format(DateUtils.addDays(new Date(), -days)));
                    result = twitter.search(query);
                    tweetList = result.getTweets();

                    FSDataOutputStream outputStream =fs.create(new Path(dst+"/"+"file"+(days-1)));

                    for (Status tweet : tweetList){
                        outputStream.writeBytes("@"+tweet.getUser().getScreenName() + "|" + tweet.getText()+"|"+tweet.getCreatedAt());
                        }
                    outputStream.close();
                    System.out.println("File writing to" + dst + "completed.");
                }

                System.out.println(tweetList.size() + " tweets gathered successfully!");

            } catch(TwitterException te) {
                te.printStackTrace();
                System.out.println("Failed to get timeline: " + te.getMessage());
                System.exit(-1);
            }

            System.out.println(tweetList);

        }

    }

