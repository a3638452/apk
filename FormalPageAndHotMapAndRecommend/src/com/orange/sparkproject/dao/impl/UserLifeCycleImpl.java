package com.orange.sparkproject.dao.impl;

import com.orange.sparkproject.dao.UserLifeCycleDAO;
import com.orange.sparkproject.domain.UserLifeCycle;
import com.orange.sparkproject.jdbc.JDBCHelper;



public class UserLifeCycleImpl implements UserLifeCycleDAO {
	public void insert(UserLifeCycle userLifeCycle) {
		String sql = "insert into t_user_life_cycle values(?,?,?,?,?,?,?,?)";  
		
		Object[] params = new Object[]{
				userLifeCycle.getUser_id(),
				userLifeCycle.getS_xiaoxincode(),
				userLifeCycle.getS_account(),
				userLifeCycle.getLogin_count(),
				userLifeCycle.getUse_time(),
				userLifeCycle.getS_register_time()
		};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}
}
