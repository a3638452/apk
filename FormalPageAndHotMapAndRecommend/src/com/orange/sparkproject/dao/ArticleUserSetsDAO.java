package com.orange.sparkproject.dao;

import java.util.List;

import com.orange.sparkproject.domain.ArticleUserSets;



/**
 * 插入实时数据的DAO接口
 * @author Administrator
 *
 */
public interface ArticleUserSetsDAO {

	void insert(ArticleUserSets articleUserSets);
	
	void insertBatch(List<ArticleUserSets> articleUserSets);
	
}
