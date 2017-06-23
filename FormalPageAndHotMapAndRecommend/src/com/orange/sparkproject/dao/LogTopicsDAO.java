package com.orange.sparkproject.dao;

import java.util.List;

import com.orange.sparkproject.domain.LogTopics;

/**
 * 插入实时日志数据的DAO接口
 * @author Administrator
 *
 */
public interface LogTopicsDAO {
 
	void insert(LogTopics logTopics);
	void insertBatch(List<LogTopics> logTopics);
}





