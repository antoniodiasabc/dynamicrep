package br.com.atech.domain;

import java.util.List;

public class WrongMessage {
	String idioma;
	String data_inicial;
	String data_final;
	String filtro_regiao;
	String filtro_geral;
	String formato_relatorio;
	String caminho_armazenar;
	String subordinacao;
	List<WronMessageReceived> mensagens;
	
	public String getCaminho_armazenar() {
		return caminho_armazenar;
	}
	public String getData_final() {
		return data_final;
	}
	public String getData_inicial() {
		return data_inicial;
	}
	public String getFiltro_geral() {
		return filtro_geral;
	}
	public String getFiltro_regiao() {
		return filtro_regiao;
	}
	
	public String getFormato_relatorio() {
		return formato_relatorio;
	}
	
	public String getIdioma() {
		return idioma;
	}
	public List<WronMessageReceived> getMensagens() {
		return mensagens;
	}
	public String getSubordinacao() {
		return subordinacao;
	}
	public void setCaminho_armazenar(String caminho_armazenar) {
		this.caminho_armazenar = caminho_armazenar;
	}
	public void setData_final(String data_final) {
		this.data_final = data_final;
	}
	public void setData_inicial(String data_inicial) {
		this.data_inicial = data_inicial;
	}
	public void setFiltro_geral(String filtro_geral) {
		this.filtro_geral = filtro_geral;
	}
	public void setFiltro_regiao(String filtro_regiao) {
		this.filtro_regiao = filtro_regiao;
	}
	public void setFormato_relatorio(String formato_relatorio) {
		this.formato_relatorio = formato_relatorio;
	}
	public void setIdioma(String idioma) {
		this.idioma = idioma;
	}
	public void setMensagens(List<WronMessageReceived> mensagens) {
		this.mensagens = mensagens;
	}
	public void setSubordinacao(String subordinacao) {
		this.subordinacao = subordinacao;
	}
}


/*

[ {
"IDIOMA": "PT",
"DATA_INICIAL": "String da dataInicial usada no filtro",
"DATA_FINAL": "String da dataFinal usada no filtro",
"FILTRO_REGIAO": "País: XX Aeroportos Extras",
"FILTRO_GERAL": "PAÍS",
"MENSAGENS": [
{
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