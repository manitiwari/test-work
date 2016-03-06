/// \file pivot.cpp
/// \ingroup table_functions
/// \brief A DATATRANSFORM table function that pivots a table column to horizontal based on the query provided
///
/// \b Synopsis
///
/// PIVOT ( ON table_reference WITH PIVOTCOL ( pivot_column ) PIVOTVAL ( value_column ) GROUPCOL ( groupcolumn ) COLUMN_LIST ( 'query of distinct value' ) )
///
/// The key column is dynamically mapped to a value and appended to the output.  The map is defined by a database table.
///
/// <b>Named Parameters</b>
///
/// PIVOTCOL is required and must be a column reference to the ON clause table_reference.
///
/// GROUPCOL is required and must be a column reference to the ON clause table_reference.
///
/// COLUMN_LIST is required and must be a string that represents a query that maps the column values to be the column_names to map to.
///
/// \b Example

#include <cstdio>
#include <unordered_map>
#include <sstream>
#include <string>
#include <limits>
#include "vdb_udf.hpp"
#include "vdb_udf_sql_client.hpp"

#define NPV_COLQRY "column_list"
#define NPV_GROUPCOL "groupcol"
#define NPV_PIVOTCOL "pivotcol"
#define NPV_PIVOTVAL "pivotval"

std::ostream& operator <<(std::ostream& ostr, __int128_t bigint)
{
	if (bigint < 0)
		return ostr << '-' << bigint*-1;
	if (bigint > std::numeric_limits<long long>::max())
		return ostr << bigint / 1000000000000LL << (long long) (bigint%1000000000000LL);
	return ostr << (long)(bigint);
}

/// The key-value pair map is a session object.  Map entries are added at the Start command.
class PivotMapTable : public vdb_udf::SessionObject
{
public:

    vdb_udf::int_t m_key_col_idx;  
    vdb_udf::int_t m_value_type;
    vdb_udf::int_t m_value_len;
    vdb_udf::int_t m_pivotcol_type;
    vdb_udf::int_t m_pivotcol_len;
    typedef std::unordered_map<std::string, vdb_udf::int_t> unordered_pmap; 
    unordered_pmap m_map_values;

    void serialize(vdb_udf::Serializer &s)
    {
        // Serialize key-value column indices from named parameters
        vdb_udf::int_t entries = m_map_values.size();

        // Serialize metadata
        s << m_pivotcol_type << m_pivotcol_len << m_value_type << m_value_len << entries;

        // Serialize key-value pair
        for (unordered_pmap::const_iterator it = m_map_values.begin(); it != m_map_values.end(); ++it)
        {
			std::string *data = new std::string(it->first);
            s << *((std::string*)data);
			s << (vdb_udf::int_t)it->second;
        }
    }

    void deserialize(vdb_udf::Serializer &s) 
    {
        vdb_udf::int_t entries;

        // Same order as serialize
        s >> m_pivotcol_type >> m_pivotcol_len >> m_value_type >> m_value_len >> entries;

		vdb_udf::int_t data;

        for (vdb_udf::int_t i=0; i<entries; i++)
        {      
			std::string *val = new std::string;
            s >> *val ;
			s >> data;
			m_map_values.insert(unordered_pmap::value_type(*val, data));
        }
    }

    void setPivotValType(vdb_udf::int_t v, vdb_udf::int_t len)
    {
        m_value_type = v;
        m_value_len = len;
    }
    void setPivotColType(vdb_udf::int_t v, vdb_udf::int_t len)
    {
		m_pivotcol_type = v;
		m_pivotcol_len = len;
    }

    void add(vdb_udf::RowDesc *row_p, vdb_udf::int_t colpos) 
    {
	std::string key; 
	std::ostringstream keycvt;
	vdb_udf::timestamp_t mytimestamp;
	vdb_udf::bigint_t mybigint;
	vdb_udf::numeric_t mynumeric;
	vdb_udf::int_t myint;
	vdb_udf::date_t mydate;
	vdb_udf::smallint_t mysmallint;
	vdb_udf::float4_t myfloat4;
	vdb_udf::float8_t myfloat8;
	vdb_udf::bool_t convert_final = true;
	switch(m_pivotcol_type)
	{
	    case vdb_udf::TypeTimeStamp:
		mytimestamp = row_p->getTimeStamp(0);	
		keycvt << (vdb_udf::bigint_t) mytimestamp;
		break;
            case vdb_udf::TypeBigInt:
                mybigint = row_p->getBigInt(0);
				keycvt << mybigint;
                break;
            case vdb_udf::TypeNumeric:
                mynumeric = row_p->getNumeric(0);
				keycvt << (vdb_udf::numeric_t) mynumeric;
                break;
            case vdb_udf::TypeInt:
                myint = row_p->getInt(0);
				keycvt << myint;
                break;
            case vdb_udf::TypeDate:
                mydate = row_p->getDate(0);
				keycvt << mydate;
                break;
            case vdb_udf::TypeSmallInt:
                mysmallint = row_p->getSmallInt(0);
				keycvt << mysmallint;
                break;
            case vdb_udf::TypeFloat4:
                myfloat4 = row_p->getFloat4(0);
				keycvt << myfloat4;
                break;
            case vdb_udf::TypeFloat8:
                myfloat8 = row_p->getFloat8(0);
				keycvt << myfloat8;
                break;
            case vdb_udf::TypeVarChar:
            case vdb_udf::TypeBpChar:
                row_p->getValueAsString(0, key);
				convert_final = false;
                break;
            default:
                break;
	}

	if (convert_final)
	    key = keycvt.str();
		m_map_values.insert(unordered_pmap::value_type(key, colpos));
    }

    vdb_udf::int_t findcolumnoffset(vdb_udf::TableArg &arg, std::string key)
    {
        unordered_pmap::iterator it = m_map_values.find(key);

        // Set to null if no match found
        if (it == m_map_values.end())
        {
            return(-1);
        }
		else
		{
			return(it->second);
		}
    }

    inline vdb_udf::int_t keyCol() {return m_key_col_idx;}
    inline void setKeyCol(vdb_udf::int_t idx) {m_key_col_idx = idx;}
    inline std::size_t getMapSize() { return m_map_values.size(); }
    inline vdb_udf::int_t getPivotValType() { return m_value_type; }
    inline vdb_udf::int_t getPivotValLen()  { return m_value_len; }

    PivotMapTable()
    {
        m_key_col_idx = 0;
        m_value_type = 0;    
        m_value_len = 0;
		m_pivotcol_type = 0;
		m_pivotcol_len = 0;
    }

    ~PivotMapTable()
    {
        unordered_pmap::iterator it = m_map_values.erase( m_map_values.begin(), m_map_values.end());
    }
};


class PivotClass : public vdb_udf::TableFunction
{
    PivotMapTable                m_map;
    typedef std::vector<vdb_udf::Column *> ColumnVector;
    typedef std::vector<vdb_udf::Column *>::iterator ColumnVectorIterator;
    typedef struct 
    {
		vdb_udf::ColumnIndexVector grpCols;
		vdb_udf::ColumnIndex pivotColIdx;
		vdb_udf::int_t pivotColType;
		vdb_udf::ColumnIndexVector pivotValCols;
		vdb_udf::int_t numpivotValCols;
		std::string collistquery;
		std::vector <vdb_udf::Column *> pivotValColDescs;
    } PivotParameters;

protected:
    PivotParameters m_pivotParameters;
    vdb_udf::bool_t m_first_time;
    vdb_udf::RowDesc *m_out_rd;
    vdb_udf::RowStore &m_store;

public:  
    PivotClass(vdb_udf::TableArg &arg, PivotParameters &pivotParameters) : m_store(arg.getRowStore()), m_first_time(true), m_pivotParameters(pivotParameters)
    {
        m_out_rd = m_store.alloc();
        arg.getSessionData(m_map);
    }

    ~PivotClass()
    {
        m_store.free(m_out_rd);
    }

    void process(vdb_udf::TableArg &arg, vdb_udf::RowDesc *rd_in)
    {
		vdb_udf::ColumnIndex outIdx = 0;
		vdb_udf::int_t numGrpCols = m_pivotParameters.grpCols.size();
		std::string key ; 
		std::ostringstream keycvt;
		vdb_udf::timestamp_t mytimestamp;
		vdb_udf::bigint_t mybigint;
		vdb_udf::numeric_t mynumeric;
		vdb_udf::int_t myint;
		vdb_udf::date_t mydate;
		vdb_udf::smallint_t mysmallint;
		vdb_udf::float4_t myfloat4;
		vdb_udf::float8_t myfloat8;
		vdb_udf::bool_t convert_final = true;

		if (m_first_time)
		{
// output the grouping columns
			for (vdb_udf::int_t grpIdx = 0; grpIdx < numGrpCols; grpIdx++)
			{
				int inColIdx = m_pivotParameters.grpCols[grpIdx];
				arg.copyColumnValue( rd_in, inColIdx, m_out_rd, outIdx);
				outIdx++;
			}
// set all pivoted elements to null initially
			for (vdb_udf::int_t j = 0; j < (m_map.getMapSize() * m_pivotParameters.numpivotValCols); j++)
            {		
				m_out_rd->setNull(outIdx++, true);
			}
			m_first_time = false;
		} 
	
		outIdx = numGrpCols;

		if (rd_in->isNull(m_pivotParameters.pivotColIdx))
		{
			char emsg[256];
			snprintf(emsg, 256, "Cant map NULL pivotcolumn reference");
			arg.throwError(__func__, emsg);
		}
		switch(m_pivotParameters.pivotColType)
		{
			case vdb_udf::TypeTimeStamp:
				mytimestamp = rd_in->getTimeStamp(m_pivotParameters.pivotColIdx);	
				keycvt << (vdb_udf::bigint_t) mytimestamp;
				break;
            case vdb_udf::TypeBigInt:
                mybigint = rd_in->getBigInt(m_pivotParameters.pivotColIdx);
				keycvt << mybigint;
                break;
            case vdb_udf::TypeNumeric:
                mynumeric = rd_in->getNumeric(m_pivotParameters.pivotColIdx);
				keycvt << (vdb_udf::numeric_t) mynumeric;
                break;
            case vdb_udf::TypeInt:
                myint = rd_in->getInt(m_pivotParameters.pivotColIdx);
				keycvt << myint;
                break;
            case vdb_udf::TypeDate:
                mydate = rd_in->getDate(m_pivotParameters.pivotColIdx);
				keycvt << mydate;
                break;
            case vdb_udf::TypeSmallInt:
                mysmallint = rd_in->getSmallInt(m_pivotParameters.pivotColIdx);
				keycvt << mysmallint;
                break;
            case vdb_udf::TypeFloat4:
                myfloat4 = rd_in->getFloat4(m_pivotParameters.pivotColIdx);
				keycvt << myfloat4;
                break;
            case vdb_udf::TypeFloat8:
                myfloat8 = rd_in->getFloat8(m_pivotParameters.pivotColIdx);
				keycvt << myfloat8;
                break;
            case vdb_udf::TypeVarChar:
            case vdb_udf::TypeBpChar:
                rd_in->getValueAsString(m_pivotParameters.pivotColIdx, key);
				convert_final = false;
                break;
            default:
                break;
		}

		if (convert_final)
			key = keycvt.str();

		vdb_udf::int_t myoffset = m_map.findcolumnoffset(arg, key);
		if (myoffset < 0)
		{
			char emsg[2048];
			snprintf(emsg, 2048, "Unexpected failure in finding pivotkey %s in map", key.c_str());
			arg.throwError(__func__, emsg);
		}
		for (vdb_udf::int_t c_coloffset = 0; c_coloffset < m_pivotParameters.numpivotValCols; c_coloffset++)
		{
			vdb_udf::int_t thiscolpos = outIdx+(myoffset*m_pivotParameters.numpivotValCols)+c_coloffset;
			if (thiscolpos < numGrpCols)
			{
				char emsg[256];
				snprintf(emsg, 256, "Derived offset %d does not map to proper pivot position", thiscolpos);
				arg.throwError(__func__, emsg);
			}
			arg.copyColumnValue(rd_in, m_pivotParameters.pivotValCols[c_coloffset], m_out_rd, thiscolpos);
		}
    }
   
    void flush(vdb_udf::TableArg &arg)
    {
        arg.getRowStore().put(m_out_rd);
    }

    static void validate(vdb_udf::TableArg &arg, PivotParameters *pivotParameters, vdb_udf::bool_t start_cmd )
    {
        const vdb_udf::NamedParameterValue *npvPivotCol = arg.getNamedParameterValue( NPV_PIVOTCOL );
        const vdb_udf::NamedParameterValue *npvGrpCol = arg.getNamedParameterValue ( NPV_GROUPCOL );
		const vdb_udf::NamedParameterValue *npvColQuery = arg.getNamedParameterValue ( NPV_COLQRY );
		const vdb_udf::NamedParameterValue *npvPivotVal = arg.getNamedParameterValue ( NPV_PIVOTVAL );

    	if (npvPivotCol == NULL)
    	{
			char emsg[256];
			snprintf( emsg, 256, "\'%s\' must be specified.", NPV_PIVOTCOL );
			arg.throwError(__func__, emsg);
        }
        else
        {
			if ( npvPivotCol->kindOfParameter() != vdb_udf::npColRef )
			{
				char emsg[256];
				snprintf(emsg, 256, "\'%s\' must be a column reference.", NPV_PIVOTCOL );
				arg.throwError(__func__, emsg);
			}
			else
			{
				pivotParameters->pivotColIdx = npvPivotCol->getColRef();
				if (!start_cmd)
				{
					pivotParameters->pivotColType = arg.getInputColumn(pivotParameters->pivotColIdx)->type;
				}
			}		
        }
        if (npvGrpCol == NULL)
        {
			char emsg[256];
			snprintf( emsg, 256, "\'%s\' must be specified.", NPV_GROUPCOL );
			arg.throwError(__func__, emsg);
        }
        else
        {
			npvGrpCol->fillColumnIndexVector( pivotParameters->grpCols );
        }
		if (npvColQuery == NULL)
		{
			char emsg[256];
			snprintf( emsg, 256, "\'%s\' must be specified.", NPV_COLQRY );
			arg.throwError(__func__, emsg);
		}
		else
		{
			npvColQuery->getValueAsString( pivotParameters->collistquery );
		}
		if (npvPivotVal == NULL )
		{
			char emsg[256];
			snprintf( emsg, 256, "\'%s\' must be specified.", NPV_COLQRY );
			arg.throwError(__func__, emsg);
		}
		else
		{
			if (npvPivotVal->kindOfParameter() != vdb_udf::npConst)
			{
				npvPivotVal->fillColumnIndexVector( pivotParameters->pivotValCols );
				pivotParameters->numpivotValCols = pivotParameters->pivotValCols.size();
				if (!start_cmd)
				{
					pivotParameters->pivotValColDescs.reserve(pivotParameters->numpivotValCols);
					for (vdb_udf::int_t pvalIdx = 0; pvalIdx < pivotParameters->numpivotValCols; pvalIdx++)
					{
						pivotParameters->pivotValColDescs[pvalIdx] = arg.getInputColumn(pivotParameters->pivotValCols[pvalIdx]);
					}
				}
			}
			else
			{
				char emsg[256];
				snprintf(emsg, 256, "\'%s\' must be a column reference or list of column references", NPV_PIVOTVAL );
				arg.throwError(__func__, emsg);
			}	
		}
    }
   
    static void DescribeCmd(vdb_udf::TableArg &arg)
    { 
        PivotClass::PivotParameters pivotParameters;
		validate(arg, &pivotParameters, false);

        vdb_udf::SQLClient sql(arg);
		vdb_udf::RowDesc *rowp;
        vdb_udf::ColumnIndex outidx = 0;
		vdb_udf::int_t numGrpCols = pivotParameters.grpCols.size();
		vdb_udf::ColumnIndex thisidx = 0;
		vdb_udf::int_t colcount = 0;

		for (vdb_udf::int_t grpIdx = 0; grpIdx < numGrpCols; grpIdx++)
		{
			int inColIdx = pivotParameters.grpCols[grpIdx];
			arg.addPartitionByColumn( inColIdx );
			arg.addOrderByColumn( inColIdx );
			arg.copyColumnSchema ( inColIdx );
			outidx++;
		} 

		arg.setGlobalPartitioning( true );
	
        vdb_udf::Schema &schema = sql.open(pivotParameters.collistquery.c_str());

        if (schema.size() < pivotParameters.numpivotValCols+1)
        {
			char emsg[256];
			snprintf(emsg, 256, "invalid column description query, must have at least %d columns", pivotParameters.numpivotValCols+1);
            arg.throwError(__func__, emsg); 
        }

		for (vdb_udf::int_t s_colcount = 0; s_colcount < pivotParameters.numpivotValCols; s_colcount++)
		{
			if (!(schema.at(s_colcount+1)->type == vdb_udf::TypeVarChar || schema.at(s_colcount+1)->type == vdb_udf::TypeBpChar))
			{
	    		char emsg[256];
	    		snprintf(emsg, 256, "invalid column description query, column %d must be a string", s_colcount+1);
            	arg.throwError(__func__, emsg); 
			}
		}

		while ( (rowp = sql.fetch()) != NULL )
		{
			for (vdb_udf::int_t r_colcount = 0; r_colcount < pivotParameters.numpivotValCols; r_colcount++)
			{
				std::string val;
		
				thisidx = arg.addOutputColumn(pivotParameters.pivotValColDescs[r_colcount]->type, pivotParameters.pivotValColDescs[r_colcount]->length,
				pivotParameters.pivotValColDescs[r_colcount]->nullable, pivotParameters.pivotValColDescs[r_colcount]->precision, pivotParameters.pivotValColDescs[r_colcount]->scale);
				rowp->getValueAsString(r_colcount+1, val);
				arg.getOutputColumn(thisidx)->name.assign(val);
			}
			colcount++;
		}
	
        arg.enableSessionCommands();
    }

    static void StartCmd(vdb_udf::TableArg &arg)
    {
        vdb_udf::SQLClient sql(arg);
        PivotMapTable tblMap;    
        PivotClass::PivotParameters pivotParameters;
		vdb_udf::int_t coloffset = 0;
        vdb_udf::RowDesc *rowp;

		validate(arg, &pivotParameters, true);

        vdb_udf::Schema &schema = sql.open(pivotParameters.collistquery.c_str());
        vdb_udf::Column *val_col_p = schema.at(0);

        tblMap.setPivotColType(val_col_p->type, val_col_p->length);

        while ( (rowp = sql.fetch()) != NULL )
        {
            tblMap.add(rowp, coloffset++);
        }

        sql.close();

        arg.setSessionData( tblMap ) ;
    }

    static void ShutdownCmd(vdb_udf::TableArg &/*arg*/)
    {
    }

    static void AbortCmd(vdb_udf::TableArg &/*arg*/)
    {
    }  

    static void FinalizeCmd(vdb_udf::TableArg &arg)
    {
		((PivotClass *)arg.getFunctor())->flush(arg);
    }

    static void CreateCmd(vdb_udf::TableArg &arg)
    {
        PivotClass::PivotParameters pivotParameters;
		validate(arg, &pivotParameters, false);	
        arg.assignFunctor( new PivotClass(arg, pivotParameters) );
    }  
};

vdb_UDF_VERSION(pivot);
extern "C" void pivot(vdb_udf::TableArg &arg)
{
    switch ( arg.getCommand() )
    {
        case vdb_udf::Describe:
            PivotClass::DescribeCmd(arg);
            break;
        case vdb_udf::Create:          
            PivotClass::CreateCmd(arg);            
            break;
        case vdb_udf::Finalize:
			PivotClass::FinalizeCmd(arg);
            break;
        case vdb_udf::Destroy:
            arg.destroyFunctor() ;
            break;
        case vdb_udf::Start:
            PivotClass::StartCmd(arg);
            break;
        case vdb_udf::Shutdown:
            PivotClass::ShutdownCmd(arg);
            break;
        case vdb_udf::Abort:
            PivotClass::AbortCmd(arg);
            break;
        default:
            break;
    }
}
