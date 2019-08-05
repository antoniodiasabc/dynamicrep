package br.com.atech.domain;

public class ParametrosRequisicao {

	String tempoInicial;
	String tempoFinal;
	String unidadeTempo;
	String tipoMsg;

	public String getTempoFinal() {
		return tempoFinal;
	}
	public void setTempoFinal(String tempoFinal) {
		this.tempoFinal = tempoFinal;
	}

	public String getTempoInicial() {
		return tempoInicial;
	}
	public void setTempoInicial(String tempoInicial) {
		this.tempoInicial = tempoInicial;
	}
	public String getUnidadeTempo() {
		return unidadeTempo;
	}
	public void setUnidadeTempo(String unidadeTempo) {
		this.unidadeTempo = unidadeTempo;
	}
	public String getTipoMsg() {
		return tipoMsg;
	}
	public void setTipoMsg(String tipoMsg) {
		this.tipoMsg = tipoMsg;
	}
}