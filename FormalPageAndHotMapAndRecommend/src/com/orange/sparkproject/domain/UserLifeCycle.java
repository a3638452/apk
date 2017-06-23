package com.orange.sparkproject.domain;

import java.io.Serializable;

public class UserLifeCycle implements Serializable{

	private static final long serialVersionUID = 1L;
	private String user_id;
	private String s_xiaoxincode;
	private String s_account;
	private String login_count;
	private String use_time;
	private String s_register_time;
	
	public String getUser_id() {
		return user_id;
	}
	public void setUser_id(String user_id) {
		this.user_id = user_id;
	}
	public String getS_xiaoxincode() {
		return s_xiaoxincode;
	}
	public void setS_xiaoxincode(String s_xiaoxincode) {
		this.s_xiaoxincode = s_xiaoxincode;
	}
	public String getS_account() {
		return s_account;
	}
	public void setS_account(String s_account) {
		this.s_account = s_account;
	}
	public String getLogin_count() {
		return login_count;
	}
	public void setLogin_count(String login_count) {
		this.login_count = login_count;
	}
	public String getUse_time() {
		return use_time;
	}
	public void setUse_time(String use_time) {
		this.use_time = use_time;
	}
	public String getS_register_time() {
		return s_register_time;
	}
	public void setS_register_time(String s_register_time) {
		this.s_register_time = s_register_time;
	}
	
	
	
}
