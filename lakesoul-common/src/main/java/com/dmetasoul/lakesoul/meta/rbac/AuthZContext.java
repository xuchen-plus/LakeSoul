package com.dmetasoul.lakesoul.meta.rbac;

import com.dmetasoul.lakesoul.meta.DBUtil;

public class AuthZContext {
    private static final AuthZContext CONTEXT =  new AuthZContext();

    private AuthZContext(){

    }

    public static AuthZContext getInstance(){
        return CONTEXT;
    }


    public String getSubject() {
        return DBUtil.getDBInfo().getUsername();
    }

    public String getDomain() {
        return DBUtil.getDomain();
    }

}

