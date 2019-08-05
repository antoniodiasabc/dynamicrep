package br.com.atech.domain;

import java.util.List;

public class WronMessageReceived {
	String id_remetente;
	String dt_hr_erro;
	String status_ori;
	String prioridade_ori;
	String dt_correcao_ori;
	String desc_erro_ori;
	String desc_mens;
	List<CorrectedMessage> correcoes;
	
	public List<CorrectedMessage> getCorrecoes() {
		return correcoes;
	}
	public String getDesc_erro_ori() {
		return desc_erro_ori;
	}
	public String getDesc_mens() {
		return desc_mens;
	}
	public String getDt_correcao_ori() {
		return dt_correcao_ori;
	}
	public String getDt_hr_erro() {
		return dt_hr_erro;
	}
	public String getId_remetente() {
		return id_remetente;
	}
	public String getPrioridade_ori() {
		return prioridade_ori;
	}
	public String getStatus_ori() {
		return status_ori;
	}

	public void setCorrecoes(List<CorrectedMessage> correcoes) {
		this.correcoes = correcoes;
	}
	public void setDesc_erro_ori(String desc_erro_ori) {
		this.desc_erro_ori = desc_erro_ori;
	}
	public void setDesc_mens(String desc_mens) {
		this.desc_mens = desc_mens;
	}
	public void setDt_correcao_ori(String dt_correcao_ori) {
		this.dt_correcao_ori = dt_correcao_ori;
	}
	public void setDt_hr_erro(String dt_hr_erro) {
		this.dt_hr_erro = dt_hr_erro;
	}
	public void setId_remetente(String id_remetente) {
		this.id_remetente = id_remetente;
	}
	public void setPrioridade_ori(String prioridade_ori) {
		this.prioridade_ori = prioridade_ori;
	}
	public void setStatus_ori(String status_ori) {
		this.status_ori = status_ori;
	}
	
}

/*
"SUBORDINACAO": "Subordina¿¿o: AEROPORTOS EXTRAS (SAM)",
"ID_REMETENTE": "SPIMYZYX",
"DT_HR_ERRO": "2017-06-20T23:49:47",
"STATUS_ORI": "INCORRETA",
"PRIORIDADE_ORI": "GG",
"DT_CORRECAO_ORI": "20/06/2017 23:49",
"DESC_ERRO_ORI": " Cabeçalho Inválido.\n",
"DESC_MENS": "\u0002FTPR88 SPIM 202300\nTAF SPHO 202300Z NIL=\n",
"CORRECOES": [

*/