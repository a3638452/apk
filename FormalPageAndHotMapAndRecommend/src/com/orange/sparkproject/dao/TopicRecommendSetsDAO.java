package com.orange.sparkproject.dao;

import java.util.List;

import com.orange.sparkproject.domain.TopicRecommendSets;



/**
 * 插入实时数据的DAO接口
 * @author Administrator
 *
 */
public interface TopicRecommendSetsDAO {

	void insert(TopicRecommendSets topicRecommendSets);
	
	/**
	 * 批量插入
	 * @param topicRecommendSets
	 */
	void insertBatch(List<TopicRecommendSets> topicRecommendSets);
	
}
