package com.orange.sparkproject.dao.impl;

import com.orange.sparkproject.dao.realTimeDAO;
import com.orange.sparkproject.domain.LoginData;
import com.orange.sparkproject.jdbc.JDBCHelper;

public class realTimeDAOImpl implements realTimeDAO {

	@Override
	public void insert(LoginData loginData) {
		String sql = "insert into realtime_logindata(userid,logintime,devicetype,devicescreen,devicenetwork,province,city,area,streetarea,lng,lat) values(?,?,?,?,?,?,?,?,?,?,?)";  
		Object[] params = new Object[]{
				loginData.getUserid(),
				loginData.getLogintime(),
				loginData.getDevicetype(),
				loginData.getDevicescreen(),
				loginData.getDevicenetwork(),
				loginData.getProvince(),
				loginData.getCity(),
				loginData.getArea(),
				loginData.getStreetarea(),
				loginData.getLng(),
				loginData.getLat()
		};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}
}

