package br.com.atech.domain;

import java.io.Serializable;

import javax.persistence.Entity;
import javax.persistence.Id;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

@Entity
public class Transport implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 7373903987503232820L;
	
	@Id
	@JsonIgnore
	private int id;
	
	@JsonIgnore
	private String date;
	
	@JsonInclude
	private Long dateJs;
	
	@JsonInclude
	private int qtd;
	
	public String getDate() {
		return date;
	}
	public Long getDateJs() {
		return dateJs;
	}
	
	public int getId() {
		return id;
	}
	public int getQtd() {
		return qtd;
	}
	
	public void setDate(String date) {
		this.date = date;
	}
	public void setDateJs(Long dateJs) {
		this.dateJs = dateJs;
	}
	public void setId(int id) {
		this.id = id;
	}
	public void setQtd(int qtd) {
		this.qtd = qtd;
	}

}
