package com.orange.dao.factory;

import com.orange.dao.DayUseTimeDAO;
import com.orange.dao.HomeJumpRateDAO;
import com.orange.dao.ModuleUserCountDAO;
import com.orange.dao.PageJumpRateDAO;
import com.orange.dao.Update2DayModuleUserCountDAO;
import com.orange.dao.UpdateUserLifeCycleDAO;
import com.orange.dao.UserBaseTagsDAO;
import com.orange.dao.UserLifeCycleDAO;
import com.orange.dao.UserModuleUsetimeDAO;
import com.orange.dao.userAactiveFrequencyDAO;
import com.orange.dao.impl.DayUseTimeImpl;
import com.orange.dao.impl.HomeJumpRateImpl;
import com.orange.dao.impl.ModuleUserCountImpl;
import com.orange.dao.impl.PageJumpRateImpl;
import com.orange.dao.impl.Update2DayModuleUserCountImpl;
import com.orange.dao.impl.UpdateUserLifeCycleImpl;
import com.orange.dao.impl.UserAactiveFrequencyImpl;
import com.orange.dao.impl.UserBaseTagsImpl;
import com.orange.dao.impl.UserLifeCycleImpl;
import com.orange.dao.impl.UserModuleUsetimeImpl;

/**
 * DAO工厂类
 * @author Administrator
 *
 */
public class DAOFactory {
	

	/**
	 *  用户生命周期表执行插入mysql
	 */
	public static UserLifeCycleDAO getUserLifeCycleDAO() {
		return new UserLifeCycleImpl();
	}
	
	/**
	 *  更新用户生命周期表
	 */
	public static UpdateUserLifeCycleDAO getUpdateUserLifeCycleDAO() {
		return new UpdateUserLifeCycleImpl();
	}
	/**
	 *  用户模块使用时长表执行插入mysql
	 */
	public static UserModuleUsetimeDAO getUserModuleUsetimeDAO() {
		return new UserModuleUsetimeImpl();
	}
	
	/**
	 *  用户活躍頻數表C插入mysql
	 */
	public static userAactiveFrequencyDAO getuserAactiveFrequencyDAO() {
		return new UserAactiveFrequencyImpl();
	}
	
	/**
	 *  用户基本信息画像
	 */
	public static UserBaseTagsDAO getUserBaseTagsDAO() {
		return new UserBaseTagsImpl();
	}
	
	/**
	 *  插入昨天新注册用户模块用户量
	 */
	public static ModuleUserCountDAO getModuleUserCountDAO() {
		return new ModuleUserCountImpl();
	}
	
	/**
	 *  执行更新2天前的留存用户
	 */
	public static Update2DayModuleUserCountDAO getUpdate2DayModuleUserCountDAO() {
		return new Update2DayModuleUserCountImpl();
	}
	
	/**
	 *  用户停留时长统计执行插入mysql
	 */
	public static DayUseTimeDAO getDayUseTimeDAO() {
		return new DayUseTimeImpl();
	}
	
	/**
	 *  e学产品跳出率执行插入mysql
	 */
	public static HomeJumpRateDAO getHomeJumpRateDAO() {
		return new HomeJumpRateImpl();
	}
	
	
	/**
	 *  页面跳出率执行插入mysql
	 */
	public static PageJumpRateDAO getPageJumpRateDAO() {
		return new PageJumpRateImpl();
	}
	}
	
