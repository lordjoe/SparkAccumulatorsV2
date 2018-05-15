package com.lordjoe.testing;

import java.io.Serializable;

/**
 * This class is a good Java bean but one field holds an object
 * which is not a bean
 */
public class MyBean  implements Serializable {
    private int m_count;
    private String m_Name;
    private MyUnBean m_UnBean;

    public MyBean(int count, String name, MyUnBean unBean) {
        m_count = count;
        m_Name = name;
        m_UnBean = unBean;
    }

    public int getCount() {return m_count; }
    public void setCount(int count) {m_count = count;}
    public String getName() {return m_Name;}
    public void setName(String name) {m_Name = name;}
    public MyUnBean getUnBean() {return m_UnBean;}
    public void setUnBean(MyUnBean unBean) {m_UnBean = unBean;}
}
