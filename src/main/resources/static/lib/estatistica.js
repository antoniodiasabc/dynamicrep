function vinculaDatePicker(datetime_input_id){
	$( "#"+datetime_input_id ).datetimepicker({ 
		timeFormat: "HH:mm:ss",
		dateFormat: "yy-mm-dd"
	});
	console.log("passei aqui");
	
	//var datetime_input = document.getElementById(datetime_input_id);
	//datetime_input.datetimepicker({ 
	//	timeFormat: "HH:mm:ss",
	//	dateFormat: "yy-mm-dd"
	//});
	
	//$( "#momento_inicial" )
	
	
}

function atualizaDatepicker(datetime_input) {
	 	if (datetime_input === undefined){
	 		return;
	 	}
	 	datetime_input.datepicker("destroy");
		switch (document.getElementById("intervalo").value) {
			case "segundo":
				datetime_input.datetimepicker({ 
                		        timeFormat: "HH:mm:ss",
					dateFormat: "yy-mm-dd"
		                });
				break;
			case "minuto":
				datetime_input.datetimepicker({ 
                		        timeFormat: "HH:mm:00",
					dateFormat: "yy-mm-dd"
		                });
				break;
			case "hora":
				datetime_input.datetimepicker({ 
                		        timeFormat: "HH:00:00",
					dateFormat: "yy-mm-dd"
		                });
				break;
			case "dia":
				datetime_input.datepicker({ 
					dateFormat: "yy-mm-dd 00:00:00"
				});
				break;
			case "mes":
				datetime_input.datepicker({
					dateFormat: "yy-mm-01 00:00:00"
				});
				break;
			default:
				alert("Erro com o calendário");
				break;
		}
	}