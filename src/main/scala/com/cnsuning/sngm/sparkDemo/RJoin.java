package com.cnsuning.sngm.sparkDemo;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class RJoin {
    static class RJoinMapper extends Mapper<LongWritable, Text, Text, InfoBean>{
        InfoBean bean = new InfoBean();
        Text k = new Text();

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");
            String pid = "";

            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            String name = inputSplit.getPath().getName();
            if (name.startsWith("order")) {
                pid = fields[2];
                bean.set(fields[0], fields[1], pid, Integer.parseInt(fields[3]), "", "", -1, "0");
            } else {
                pid = fields[0];
                bean.set("", "", pid, -1, fields[1], fields[2], Float.parseFloat(fields[3]), "1");
            }
            k.set(pid);
            context.write(k, bean);
        }
    }

    static class RJoinReducer extends Reducer<Text, InfoBean, InfoBean, NullWritable>{
        protected void reduce(Text pid, Iterable<InfoBean> values, Context context) throws IOException, InterruptedException {
            InfoBean pdBean = new InfoBean();
            List<InfoBean> orderBeans = new ArrayList<InfoBean>();

            for (InfoBean bean : values) {
                if ("1".equals(bean.getFlag())) { //产品
                    try {
                        BeanUtils.copyProperties(pdBean, bean);
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        e.printStackTrace();
                    }
                } else {
                    InfoBean orderBean = new InfoBean();
                    try {
                        BeanUtils.copyProperties(orderBean, bean);
                        orderBeans.add(orderBean);
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        e.printStackTrace();
                    }
                }
            }

            // 拼接两类数据形成最终结果
            for (InfoBean bean : orderBeans) {
                bean.setPname(pdBean.getPname());
                bean.setCategory_id(pdBean.getCategory_id());
                bean.setPrice(pdBean.getPrice());

                context.write(bean, NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 指定本程序的jar包所在的本地路径
        job.setJarByClass(RJoin.class);

        //System.setProperty("hadoop.home.dir", "D:\\hadoop-2.6.5");

        // 指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(RJoinMapper.class);
        job.setReducerClass(RJoinReducer.class);

        // 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(InfoBean.class);

        job.setOutputKeyClass(InfoBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }
}
