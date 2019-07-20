import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRank {
    static class DescSort extends WritableComparator{
        //method
        public DescSort(){
            super(FloatWritable.class,true);
        }

        //Override method
        @Override
        public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3, int arg4, int arg5){
            return -super.compare(arg0,arg1,arg2,arg3,arg4,arg5);
        }
        @Override
        public int compare(Object a, Object b){
            return -super.compare(a,b);
        }
    }

    static class MyMapper extends Mapper<Object, Text, Text, Text>{

        //variables
        private String id;     //当前人物id
        private float pr;          //当前人物rp值,初始为1;

        //methods
        public void map(Object key, Text value, Context context){
            //value: ID id1:rank;id2:rank;id3:rank;
            //get ID
            StringTokenizer str = new StringTokenizer(value.toString());
            if(str.hasMoreTokens()){
                id = str.nextToken();
            }else {
                return;
            }
            Text ID = new Text(id);
            //at init text, each id's pr=1;
            pr = 1.0f;
            Text PR = new Text("#"+Float.toString(pr));
            //emit ID+#+PR
            try {
                context.write(ID,PR);
            }catch (Exception e){
                e.printStackTrace();
            }

            //get next: id:rank...
            String next = str.nextToken();
            Text idsAndRanks = new Text("$"+next);
            try{
                context.write(ID,idsAndRanks);
            }catch (Exception e){
                e.printStackTrace();
            }
            StringTokenizer str2 = new StringTokenizer(next,";");
            while(str2.hasMoreTokens()){
                String ids = str2.nextToken();
                //emit id+"@"+pr;
                String name = ids.split(":")[0];
                String rank = ids.split(":")[1];
                Text k = new Text(name);
                Text v = new Text("@"+rank);
                try{
                    context.write(k,v);
                }catch (Exception e){
                    e.printStackTrace();
                }

            }

        }
    }

    static class  Mycirculation extends Mapper<Object, Text, Text, Text>{
        //variables
        private String id;     //当前人物id
        private float pr;          //当前人物rp值,初始为value[1];

        //methods
        public void map(Object key, Text value, Context context){
            //value: ID id1:rank;id2:rank;id3:rank;
            //get ID
            StringTokenizer str = new StringTokenizer(value.toString());
            if(str.hasMoreTokens()){
                id = str.nextToken();
            }else {
                return;
            }
            Text ID = new Text(id);
            //at init text, each id's pr=1;
            pr = Float.parseFloat(str.nextToken());
            Text PR = new Text("#"+Float.toString(pr));
            //emit ID+#+PR
            try {
                context.write(ID,PR);
            }catch (Exception e){
                e.printStackTrace();
            }

            //get next: id:rank...
            String next = str.nextToken();
            Text idsAndRanks = new Text("$"+next);

            //emit ID+"$"+id:rank;id:rank;...
            try{
                context.write(ID,idsAndRanks);
            }catch (Exception e){
                e.printStackTrace();
            }
            StringTokenizer str2 = new StringTokenizer(next,";");
            while(str2.hasMoreTokens()){
                String ids = str2.nextToken();

                //emit id+"@"+pr;
                String name = ids.split(":")[0];
                String rank = ids.split(":")[1];
                Float ranker = Float.parseFloat(rank);
                ranker = ranker*pr;
                Text k = new Text(name);
                Text v = new Text("@"+ranker.toString());
                try{
                    context.write(k,v);
                }catch (Exception e){
                    e.printStackTrace();
                }

            }

        }
    }

    static class MyEndMap extends Mapper<Object, Text, FloatWritable, Text>{
        public void map(Object key, Text value, Context context){
            StringTokenizer str = new StringTokenizer(value.toString());
            String ID = str.nextToken();
            Float PR = Float.parseFloat(str.nextToken());
            try {
                context.write(new FloatWritable(PR),new Text(ID));
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    static class MyReducer extends Reducer<Text, Text, Text, Text>{

        //methods
        public void reduce(Text key,Iterable<Text> values, Context context){
            //init:上次的rp值,相关人物及rank值,本次rp值.
            double prepr = 0;
            String ids = "";
            float pr = 0;
            int count = 0;

            for(Text id:values){
                String idd = id.toString();
                //rp值则先相加
                if(idd.substring(0,1).equals("@")){
                    pr+=Float.parseFloat(idd.substring(1));
                    count++;
                //相关人物及rank值则放在ids中
                }else if(idd.substring(0,1).equals("$")){
                    ids = idd.substring(1);
                //上一次的rp值
                }else if(idd.substring(0,1).equals("#")){
                    prepr = Double.parseDouble(idd.substring(1));
                }
            }
            //计算新rp值
            pr = pr;

            //输出结果
            String result = pr+" "+ids;
            try{
                context.write(key,new Text(result));
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    static class MyEndReduce extends Reducer<FloatWritable, Text, FloatWritable, Text>{
        public void reduce(FloatWritable key,Iterable<Text> values, Context context){
            for(Text id: values){
                try {
                    context.write(key, id);
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        //first time
        Job job = new Job(configuration);
        job.setJarByClass(PageRank.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]+"init"));

        job.waitForCompletion(true);

        String jobPath = args[1]+"init";
        String outPath = args[1];
        //next 10 times
        for(int i = 0; i < 10; i++){
            Job job1 = new Job(configuration);
            job1.setJarByClass(PageRank.class);

            job1.setMapperClass(Mycirculation.class);
            job1.setReducerClass(MyReducer.class);
            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(Text.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job1,new Path(jobPath));
            FileOutputFormat.setOutputPath(job1,new Path(outPath+i));
            jobPath = outPath+i;
            job1.waitForCompletion(true);
        }

        Job job2 = new Job(configuration);
        job2.setJarByClass(PageRank.class);

        job2.setSortComparatorClass(DescSort.class);
        job2.setMapperClass(MyEndMap.class);
        job2.setReducerClass(MyEndReduce.class);
        job2.setMapOutputKeyClass(FloatWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(FloatWritable.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2,new Path(jobPath));
        FileOutputFormat.setOutputPath(job2,new Path(outPath));
        job2.waitForCompletion(true);
    }
}
