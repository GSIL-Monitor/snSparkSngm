package com.cnsuning.sngm.sparkDemo;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class InfoBean implements Writable {
    private String order_id;
    private String date;
    private String pid;
    private int amount;
    private String pname;
    private String category_id;
    private float price;
    // flag=0表示这个对象是封装订单表记录
    // flag=1表示这个对象是封装产品信息记录
    private String flag;

    public void set(String order_id, String date, String pid, int amount, String pname,
                    String category_id, float price, String flag) {
        this.order_id = order_id;
        this.date = date;
        this.pid = pid;
        this.amount = amount;
        this.pname = pname;
        this.category_id = category_id;
        this.price = price;
        this.flag = flag;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.order_id = in.readUTF();
        this.date = in.readUTF();
        this.pid = in.readUTF();
        this.amount = in.readInt();
        this.pname = in.readUTF();
        this.category_id = in.readUTF();
        this.price = in.readFloat();
        this.flag = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(order_id);
        out.writeUTF(date);
        out.writeUTF(pid);
        out.writeInt(amount);
        out.writeUTF(pname);
        out.writeUTF(category_id);
        out.writeFloat(price);
        out.writeUTF(flag);
    }

    @Override
    public String toString() {
        return "order_id=" + order_id + ", date=" + date + ", pid=" + pid + ", amount=" + amount + ", pname="
                + pname + ", category_id=" + category_id + ", price=" + price;
    }

    public String getOrder_id() {
        return order_id;
    }

    public void setOrder_id(String order_id) {
        this.order_id = order_id;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public String getCategory_id() {
        return category_id;
    }

    public void setCategory_id(String category_id) {
        this.category_id = category_id;
    }

    public float getPrice() {
        return price;
    }

    public void setPrice(float price) {
        this.price = price;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }
}
