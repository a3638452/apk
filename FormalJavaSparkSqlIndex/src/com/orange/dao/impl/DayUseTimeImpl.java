package com.orange.dao.impl;

import com.orange.bean.DayUseTime;
import com.orange.dao.DayUseTimeDAO;
import com.orange.helper.JDBCHelper;

public class DayUseTimeImpl implements DayUseTimeDAO{

	@Override
	public void insert(DayUseTime dayUseTime) {
String sql = "insert into t_report_user_staytime(stay_time,user_count,report_date) values(?,?,?)";  
		
		Object[] params = new Object[]{
				dayUseTime.getStay_time(),
				dayUseTime.getUser_count(),
				dayUseTime.getReport_date()
		};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
		
	}

}
