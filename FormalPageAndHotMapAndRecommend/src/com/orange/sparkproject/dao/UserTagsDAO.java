package com.orange.sparkproject.dao;

import java.util.List;

import com.orange.sparkproject.domain.UserTags;



/**
 * 插入实时数据的DAO接口
 * @author Administrator
 *
 */
public interface UserTagsDAO {

	void insert(UserTags userTags);
	
	/**
	 * 批量插入
	 * @param 
	 */
	void insertBatch(List<UserTags> userTags);
	
}
