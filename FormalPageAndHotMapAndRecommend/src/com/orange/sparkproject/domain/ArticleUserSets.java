package com.orange.sparkproject.domain;

import java.io.Serializable;

public class ArticleUserSets implements Serializable{
	
	private static final long serialVersionUID = 1L;
		private String article_id;
		private String userset_id;
		private String recommend_type;
		private String opt_type;
		private String article_time;
		public String getArticle_id() {
			return article_id;
		}
		public void setArticle_id(String article_id) {
			this.article_id = article_id;
		}
		public String getUserset_id() {
			return userset_id;
		}
		public void setUserset_id(String userset_id) {
			this.userset_id = userset_id;
		}
		public String getRecommend_type() {
			return recommend_type;
		}
		public void setRecommend_type(String recommend_type) {
			this.recommend_type = recommend_type;
		}
		public String getOpt_type() {
			return opt_type;
		}
		public void setOpt_type(String opt_type) {
			this.opt_type = opt_type;
		}
		public String getArticle_time() {
			return article_time;
		}
		public void setArticle_time(String article_time) {
			this.article_time = article_time;
		}
		public static long getSerialversionuid() {
			return serialVersionUID;
		}
		 
		 
		 

		 

}
