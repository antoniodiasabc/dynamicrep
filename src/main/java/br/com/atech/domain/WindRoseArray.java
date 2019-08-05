package br.com.atech.domain;

public class WindRoseArray {
	private String dir;
	private String name;
	private Object [] data;
	private String _colorIndex;
	private int qtdVRB;
	private int qtdBar;
	public String get_colorIndex() {
		return _colorIndex;
	}
	public Object [] getData() {
		return data;
	}
	
	public String getDir() {
		return dir;
	}
	public String getName() {
		return name;
	}
	public int getQtdBar() {
		return qtdBar;
	}
	public int getQtdVRB() {
		return qtdVRB;
	}
	public void set_colorIndex(String _colorIndex) {
		this._colorIndex = _colorIndex;
	}
	public void setData(Object [] data) {
		this.data = data;
	}
	public void setDir(String dir) {
		this.dir = dir;
	}
	public void setName(String name) {
		this.name = name;
	}
	public void setQtdBar(int qtdBar) {
		this.qtdBar = qtdBar;
	}
	public void setQtdVRB(int qtdVRB) {
		this.qtdVRB = qtdVRB;
	}
}
