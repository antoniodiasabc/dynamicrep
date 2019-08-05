package br.com.atech.domain;

public class RequisicaoResumo {
	private String fenomeno;
	private String tempoFinal;
	private String tempoInicial;
	private String tipoMsg;
	private String unidadeTempo;
	private String localidade;
	private String idGeografico;
	private String report_type;
	private String periodo;
	private String qtd;
	private String variable;
	private String comparator;
	private String filter1;
	private String filter2;
	
	public String getComparator() {
		return comparator;
	}
	public String getFenomeno() {
		return fenomeno;
	}
	public String getFilter1() {
		return filter1;
	}
	public String getFilter2() {
		return filter2;
	}
	public String getIdGeografico() {
		return idGeografico;
	}
	public String getLocalidade() {
		return localidade;
	}
	public String getPeriodo() {
		return periodo;
	}
	public String getQtd() {
		return qtd;
	}
	
	public String getReport_type() {
		return report_type;
	}
	public String getTempoFinal() {
		return tempoFinal;
	}
	public String getTempoInicial() {
		return tempoInicial;
	}
	public String getTipoMsg() {
		return tipoMsg;
	}
	public String getUnidadeTempo() {
		return unidadeTempo;
	}
	public String getVariable() {
		return variable;
	}
	public void setComparator(String comparator) {
		this.comparator = comparator;
	}
	public void setFenomeno(String fenomeno) {
		this.fenomeno = fenomeno;
	}
	public void setFilter1(String filter1) {
		this.filter1 = filter1;
	}
	public void setFilter2(String filter2) {
		this.filter2 = filter2;
	}
	public void setIdGeografico(String idGeografico) {
		this.idGeografico = idGeografico;
	}
	public void setLocalidade(String localidade) {
		this.localidade = localidade;
	}
	public void setPeriodo(String periodo) {
		this.periodo = periodo;
	}
	public void setQtd(String qtd) {
		this.qtd = qtd;
	}
	public void setReport_type(String report_type) {
		this.report_type = report_type;
	}
	public void setTempoFinal(String tempoFinal) {
		this.tempoFinal = tempoFinal;
	}
	public void setTempoInicial(String tempoInicial) {
		this.tempoInicial = tempoInicial;
	}
	public void setTipoMsg(String tipoMsg) {
		this.tipoMsg = tipoMsg;
	}
	public void setUnidadeTempo(String unidadeTempo) {
		this.unidadeTempo = unidadeTempo;
	}
	public void setVariable(String variable) {
		this.variable = variable;
	}
}
