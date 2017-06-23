package com.orange.sparkproject.dao;

import java.util.List;

import com.orange.sparkproject.domain.TopicUserTags;


/**
 * 插入实时数据的DAO接口
 * @author Administrator
 *
 */
public interface TopicUserTagsDAO {

	void insert(TopicUserTags topicUserTags);
	
	void insertBatch(List<TopicUserTags> topicUserTags);
	
}
