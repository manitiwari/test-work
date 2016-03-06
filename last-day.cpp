#include "padb_udf.hpp"
#include <stdlib.h>
#include <string.h>

#define isleap(y) (((y) % 4) == 0 && (((y) % 100) != 0 || ((y) % 400) == 0))

#define MICROSECOND 	((padb_udf::num_microsec_t) 1000000)
PADB_UDF_VERSION(last_day)
PADB_UDF_VERSION(last_daytstamp)
PADB_UDF_VERSION(format_duration)
PADB_UDF_VERSION(normalize_time)
extern "C"
{
	void vdup(padb_udf::varchar_t *dst, char *src)
	{
		padb_udf::len_t len = strlen(src);
		padb_udf::len_t copied = 0;
		padb_udf::len_t offset = 0;
		while (copied < len)
		{
			dst->str[copied++] = src[offset++];
		}
		dst->len = len;
	}
	padb_udf::timestamp_t normalize_time(padb_udf::ScalarArg &aux, padb_udf::timestamp_t in_ts, padb_udf::timestamp_t base_ts, padb_udf::int_t interval_spec)
	{
		padb_udf::timestamp_t ret_ts;
		padb_udf::num_microsec_t ts_diff;
		padb_udf::int_t nearest_lb_multiple;
		padb_udf::int_t nearest_lb_remain;
		
		if (aux.isNull(0) || aux.isNull(1) || aux.isNull(2))
			return aux.retTimeStampNull();
		if (base_ts > in_ts)
		{
		   aux.throwError("normalize_timestamp", "Base timestamp cannot be greater than input timestamp");
		   return aux.retTimeStampNull();
		}
		ts_diff = padb_udf::microsecondsBetween(in_ts, base_ts);
		/* if we are at the interval -- just go ahead and return it */
		if (ts_diff == (padb_udf::num_microsec_t) 0)
			return aux.retTimeStampVal( in_ts );
		/* if we are less than an interval away return the base */
		if (ts_diff < (interval_spec * MICROSECOND))
			return aux.retTimeStampVal( base_ts );
		nearest_lb_remain = (padb_udf::int_t) (ts_diff % (interval_spec * MICROSECOND));
		/* we are here -- return this one */
		if (nearest_lb_remain == 0) 
			return aux.retTimeStampVal( in_ts );
		nearest_lb_multiple = (padb_udf::int_t) (ts_diff / (interval_spec * MICROSECOND));
		ret_ts = base_ts + ((padb_udf::num_microsec_t) interval_spec * nearest_lb_multiple * MICROSECOND );
		return aux.retTimeStampVal( ret_ts );
	}

	padb_udf::varchar_t *format_duration(padb_udf::ScalarArg &aux, padb_udf::int_t nsecs)
	{
		padb_udf::varchar_t *retval;
		padb_udf::int_t hours = 0;
		padb_udf::int_t minutes = 0;
		padb_udf::int_t seconds = 0;	
		char lbuf[16];
		padb_udf::len_t maxlen = 0;
		if (aux.isNull(0))
			return aux.retVarCharNull();

		memset(lbuf, 0, 16);
		if (nsecs != 0)
		{
			hours = nsecs / 3600;
			minutes = (nsecs - (hours * 3600)) / 60;
			seconds = nsecs - (hours * 3600) - (minutes * 60);	
		}
		
		snprintf(lbuf, 16, "%02d:%02d:%02d", hours, minutes, seconds);
		
		maxlen = strlen(lbuf);
		retval = aux.getRetVarCharBuf( &maxlen );

		vdup(retval, lbuf);
		return aux.retVarCharVal( retval );

	}
	padb_udf::date_t calc_lastdayofmonth(padb_udf::date_t in_date)
	{
		padb_udf::int_t m_last_days[12] = {31,28,31,30,31,30,31,31,30,31,30,31};
		padb_udf::year_t  cyear = padb_udf::extract_year_from_date(in_date);
		padb_udf::month_t  cmon = padb_udf::extract_month_from_date(in_date);
		padb_udf::day_of_month_t  cday = padb_udf::extract_day_from_date(in_date);
		padb_udf::date_t retdate;

		if (isleap(cyear) && cmon == 2)
			retdate = padb_udf::date_plus_days(in_date, (29 - cday));
		else
			retdate = padb_udf::date_plus_days(in_date, m_last_days[cmon-1]-cday);

		return retdate;
	}
		
	padb_udf::date_t last_daytstamp(padb_udf::ScalarArg &aux, padb_udf::timestamp_t in_ts)
	{
		padb_udf::date_t cdate;
		padb_udf::date_t retdate;

		if (aux.isNull(0))
			return aux.retDateNull();

		cdate = padb_udf::extract_date_from_timestamp(in_ts);

		retdate = calc_lastdayofmonth(cdate);
		
		return aux.retDateVal( retdate );
	}

	padb_udf::date_t last_day(padb_udf::ScalarArg &aux, padb_udf::date_t in_date)
	{
		padb_udf::date_t retdate;
	
		if (aux.isNull(0))
			return aux.retDateNull();

		retdate = calc_lastdayofmonth(in_date);

		return aux.retDateVal( retdate);
	}
}					
