package br.com.atech.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class Wind {
	private String direcao;
	private Double percentual;
	
	@JsonIgnore
	private int qtd;
		
	public int getQtd() {
		return qtd;
	}
	public void setQtd(int qtd) {
		this.qtd = qtd;
	}
	public String getDirecao() {
		return direcao;
	}
	public Double getPercentual() {
		return percentual;
	}
	public void setDirecao(String direcao) {
		this.direcao = direcao;
	}
	public void setPercentual(Double percentual) {
		this.percentual = percentual;
	}
}
