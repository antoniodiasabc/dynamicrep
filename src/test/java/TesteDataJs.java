import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.jpa.repository.JpaContext;
import org.springframework.test.context.junit4.SpringRunner;

import br.com.atech.controler.DynamicStatistic;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = { DynamicStatistic.class, JpaContext.class })
@EnableConfigurationProperties
class TesteDataJs {
	
	
	@Autowired
	DynamicStatistic statistic;

	@Test
	void test() {
		String dataBase = "20161220";
		long convertido = conv2Long(dataBase);
		long resp = 1482192000000l;
		assertTrue(resp == convertido);
	}

	private long conv2Long(String dataBase) {
		GregorianCalendar now = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
		int year = Integer.parseInt(dataBase.substring(0, 4));
		now.set(Calendar.YEAR, year);
		int month = Integer.parseInt(dataBase.substring(4, 6));
		now.set(Calendar.MONTH, month-1);
		int day = Integer.parseInt(dataBase.substring(6));
		now.set(Calendar.DAY_OF_MONTH, day);
		now.set(Calendar.HOUR_OF_DAY, 0);
		now.set(Calendar.MINUTE, 0);	
		now.set(Calendar.SECOND, 0);	
		now.set(Calendar.MILLISECOND, 0);
		long timeInMillis = now.getTimeInMillis();
		return timeInMillis;
	}
	
	//1482192000000
    //20122016 20161220
	
	
	
	
	
}
