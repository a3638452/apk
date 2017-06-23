package com.orange.sparkproject.dao.factory;

import com.orange.sparkproject.dao.ArticleUserSetsDAO;
import com.orange.sparkproject.dao.LogQuestionsDAO;
import com.orange.sparkproject.dao.LogTopicsDAO;
import com.orange.sparkproject.dao.TopicRecommendSetsDAO;
import com.orange.sparkproject.dao.TopicUserTagsDAO;
import com.orange.sparkproject.dao.TopicUsersetsUserDAO;
import com.orange.sparkproject.dao.UserLifeCycleDAO;
import com.orange.sparkproject.dao.UserTagsDAO;
import com.orange.sparkproject.dao.UsersetsUserDAO;
import com.orange.sparkproject.dao.realTimeDAO;
import com.orange.sparkproject.dao.impl.AndroidPageSplitConvertRateDAOImpl14Day;
import com.orange.sparkproject.dao.impl.AndroidPageSplitConvertRateDAOImpl1Day;
import com.orange.sparkproject.dao.impl.AndroidPageSplitConvertRateDAOImpl30Day;
import com.orange.sparkproject.dao.impl.AndroidPageSplitConvertRateDAOImpl7Day;
import com.orange.sparkproject.dao.impl.ArticleUserSetsDAOImpl;
import com.orange.sparkproject.dao.impl.IosPageSplitConvertRateDAOImpl14Day;
import com.orange.sparkproject.dao.impl.IosPageSplitConvertRateDAOImpl1Day;
import com.orange.sparkproject.dao.impl.IosPageSplitConvertRateDAOImpl30Day;
import com.orange.sparkproject.dao.impl.IosPageSplitConvertRateDAOImpl7Day;
import com.orange.sparkproject.dao.impl.LogQuestionsDAOImpl;
import com.orange.sparkproject.dao.impl.LogTopicsDAOImpl;
import com.orange.sparkproject.dao.impl.TopicRecommendSetsDAOImpl;
import com.orange.sparkproject.dao.impl.TopicUserTagsImpl;
import com.orange.sparkproject.dao.impl.TopicUsersetsUserImpl;
import com.orange.sparkproject.dao.impl.UserLifeCycleImpl;
import com.orange.sparkproject.dao.impl.UserTagsImpl;
import com.orange.sparkproject.dao.impl.UsersetsUserImpl;
import com.orange.sparkproject.dao.impl.realTimeDAOImpl;

/**
 * DAO工厂类
 * @author Administrator
 *
 */
public class DAOFactory {

	/**
	 * 安卓页面1天的dao层结构
	 * @return
	 */
	public static AndroidPageSplitConvertRateDAOImpl1Day getAndroidPageSplitConvertRateDAO1Day() {
		return new AndroidPageSplitConvertRateDAOImpl1Day();
	}
	/**
	
	 * 安卓页面7天的dao层结构
	 * @return
	 */
	public static AndroidPageSplitConvertRateDAOImpl7Day getAndroidPageSplitConvertRateDAO7Day() {
		return new AndroidPageSplitConvertRateDAOImpl7Day();
	}
	
	/**
	 * 安卓页面14天的dao层结构
	 * @return
	 */
	public static AndroidPageSplitConvertRateDAOImpl14Day getAndroidPageSplitConvertRateDAO14Day() {
		return new AndroidPageSplitConvertRateDAOImpl14Day();
	}
	
	/**
	 * 安卓页面30天的dao层结构
	 * @return
	 */
	public static AndroidPageSplitConvertRateDAOImpl30Day getAndroidPageSplitConvertRateDAO30Day() {
		return new AndroidPageSplitConvertRateDAOImpl30Day();
	}
	
	/**
	 * ios页面1天的dao层结构
	 */
	public static IosPageSplitConvertRateDAOImpl1Day getIosPageSplitConvertRateDAO1Day() {
		return new IosPageSplitConvertRateDAOImpl1Day();
	}
	
	/**
	 * ios页面7天的dao层结构
	 */
	public static IosPageSplitConvertRateDAOImpl7Day getIosPageSplitConvertRateDAO7Day() {
		return new IosPageSplitConvertRateDAOImpl7Day();
	}
	
	/**
	 * ios页面14天的dao层结构
	 */
	public static IosPageSplitConvertRateDAOImpl14Day getIosPageSplitConvertRateDAO14Day() {
		return new IosPageSplitConvertRateDAOImpl14Day();
	}
	
	/**
	 * ios页面30天的dao层结构
	 */
	public static IosPageSplitConvertRateDAOImpl30Day getIosPageSplitConvertRateDAO30Day() {
		return new IosPageSplitConvertRateDAOImpl30Day();
	}
	
	/**
	 * 实时longindata数据持久化到mysql
	 */
	public static realTimeDAO getRealTimeLoginData(){
		return new realTimeDAOImpl();
	}
	
	
	/**
	 *  article,用户集合数据持久化到mysql
	 */
	public static ArticleUserSetsDAO getTUserSets(){
		return new ArticleUserSetsDAOImpl();
	}
	
	/**
	 *  用户集合,用户数据持久化到mysql
	 */
	public static UsersetsUserDAO getTUser(){
		return new UsersetsUserImpl();
	}
	
	/**
	 *  执行插入用户画像mysql
	 */
	public static  UserTagsDAO getUserTags(){
		return new UserTagsImpl();
	}
	
	/**
	 *  用户生命周期指标执行插入mysql
	 */
	public static UserLifeCycleDAO getUserLifeCycleDAO() {
		return new UserLifeCycleImpl();
	}
	
	/**
	 *  topic,用户集合数据持久化到mysql
	 */
	public static TopicUserTagsDAO getTopicUserTagsDAO(){
		return new TopicUserTagsImpl();
	}
	
	/**
	 *  topic,用户集合数据持久化到mysql
	 */
	public static TopicRecommendSetsDAO getTopicRecommendSetsDAO(){
		return new TopicRecommendSetsDAOImpl();
	}
	
	/**
	 *  topic,用户集合数据持久化到mysql
	 */
	public static TopicUsersetsUserDAO getTopicUsersetsUserDAO(){
		return new TopicUsersetsUserImpl();
	}
	
	/**
	 *  清洗日志topic数据持久化到mysql
	 */
	public static LogTopicsDAO getLogTopicsDAO(){
		return new LogTopicsDAOImpl();
	}
	
	/**
	 *  清洗日志topic数据持久化到mysql
	 */
	public static LogQuestionsDAO getLogQuestionsDAO(){
		return new LogQuestionsDAOImpl();
	}
	
	
	
	}
	
