package br.com.atech.domain;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class WindRose {
	private String name;
	private Map<String, Wind> data;
	private String _colorIndex;
	
	//@JsonIgnore
	private int qtdVRB;
	
	public int getQtdVRB() {
		return qtdVRB;
	}
	public void setQtdVRB(int qtdVRB) {
		this.qtdVRB = qtdVRB;
	}
	public int getQtdBarra() {
		return qtdBarra;
	}
	public void setQtdBarra(int qtdBarra) {
		this.qtdBarra = qtdBarra;
	}
	@JsonIgnore
	private int qtdBarra;
	
	public String get_colorIndex() {
		return _colorIndex;
	}
	public void set_colorIndex(String _colorIndex) {
		this._colorIndex = _colorIndex;
	}
	public Map<String, Wind> getData() {
		return data;
	}
	public String getName() {
		return name;
	}
	public void setData(Map<String, Wind> data) {
		this.data = data;
	}
	public void setName(String name) {
		this.name = name;
	}
}
