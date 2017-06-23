package com.orange.sparkproject.dao;

import java.util.List;

import com.orange.sparkproject.domain.LogQuestions;
/**
 * 插入实时日志数据的DAO接口
 * @author Administrator
 *
 */
public interface LogQuestionsDAO {

	 void insert(LogQuestions logQuestions);
	 void insertBatch(List<LogQuestions> logQuestions);
}
