package com.orange.sparkproject.dao;

import com.orange.sparkproject.domain.LoginData;

/**
 * 插入实时数据的DAO接口
 * @author Administrator
 *
 */
public interface realTimeDAO {

	void insert(LoginData loginData);
	
}
