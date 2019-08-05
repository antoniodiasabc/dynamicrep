package br.com.atech.domain;

public class CorrectedMessage {
	String id_remetente;
	String dt_hr_erro_corr;
	String status_corr;
	String prioridade_corr;
	String dt_correcao_corr;
	String desc_erro_corr;
	String msg_corr;
	
	public String getDesc_erro_corr() {
		return desc_erro_corr;
	}
	public String getDt_correcao_corr() {
		return dt_correcao_corr;
	}
	public String getDt_hr_erro_corr() {
		return dt_hr_erro_corr;
	}
	public String getId_remetente() {
		return id_remetente;
	}
	public String getMsg_corr() {
		return msg_corr;
	}
	public String getPrioridade_corr() {
		return prioridade_corr;
	}
	public String getStatus_corr() {
		return status_corr;
	}
	public void setDesc_erro_corr(String desc_erro_corr) {
		this.desc_erro_corr = desc_erro_corr;
	}
	public void setDt_correcao_corr(String dt_correcao_corr) {
		this.dt_correcao_corr = dt_correcao_corr;
	}
	public void setDt_hr_erro_corr(String dt_hr_erro_corr) {
		this.dt_hr_erro_corr = dt_hr_erro_corr;
	}
	public void setId_remetente(String id_remetente) {
		this.id_remetente = id_remetente;
	}
	public void setMsg_corr(String msg_corr) {
		this.msg_corr = msg_corr;
	}
	public void setPrioridade_corr(String prioridade_corr) {
		this.prioridade_corr = prioridade_corr;
	}
	public void setStatus_corr(String status_corr) {
		this.status_corr = status_corr;
	}
}


/*
"ID_REMETENTE_CORR": "SPIMYZYX",
"DT_HR_ERRO_CORR": "20/06/2017 23:49",
"STATUS_CORR": "INCORRETA",
"PRIORIDADE_CORR": "GG",
"DT_CORRECAO_CORR": "20/06/2017 23:49",
"DESC_ERRO_CORR": " Cabeçalho Inválido.\n",
"MSG_CORR": "\u0002FTPR88 SPIM 202300\nTAF SPHO 202300Z NIL=\n"
}

*/