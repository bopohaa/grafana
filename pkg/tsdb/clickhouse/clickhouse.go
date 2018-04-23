package clickhouse

import (
	"container/list"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"net/url"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/bopohaa/grafana/pkg/components/null"
	"github.com/bopohaa/grafana/pkg/components/simplejson"
	"github.com/bopohaa/grafana/pkg/log"
	"github.com/bopohaa/grafana/pkg/models"
	"github.com/bopohaa/grafana/pkg/tsdb"
	_ "github.com/mailru/go-clickhouse"
)

const dateRange = ``
const dateTimeRange = ``

// ClickhouseQueryEndpoint aaa
type ClickhouseQueryEndpoint struct {
	db  *sql.DB
	log log.Logger
}

func init() {
	tsdb.RegisterTsdbQueryEndpoint("vertamedia-clickhouse-datasource", NewClickhouseQueryEndpoint)
}

// NewClickhouseQueryEndpoint aaa
func NewClickhouseQueryEndpoint(datasource *models.DataSource) (tsdb.TsdbQueryEndpoint, error) {
	u, err := url.Parse(datasource.Url)
	if err != nil {
		panic(err)
	}

	var cnnstr string
	if datasource.BasicAuth {
		user := datasource.BasicAuthUser
		if user == "" {
			user = "default"
		}

		cnnstr = fmt.Sprintf("%s://%s:%s@%s/%s",
			u.Scheme,
			url.PathEscape(user),
			url.PathEscape(datasource.BasicAuthPassword),
			u.Host,
			url.PathEscape(datasource.Database),
		)
	} else {
		cnnstr = fmt.Sprintf("%s://%s/%s",
			u.Scheme,
			u.Host,
			url.PathEscape(datasource.Database),
		)
	}

	connect, err := sql.Open("clickhouse", cnnstr)
	if err != nil {
		return nil, err
	}
	if err := connect.Ping(); err != nil {
		return nil, err
	}

	endpoint := &ClickhouseQueryEndpoint{
		log: log.New("tsdb.clickhouse"),
		db:  connect,
	}

	return endpoint, nil
}

// Query func
func (e *ClickhouseQueryEndpoint) Query(ctx context.Context, dsInfo *models.DataSource, tsdbQuery *tsdb.TsdbQuery) (*tsdb.Response, error) {

	result := &tsdb.Response{
		Results: make(map[string]*tsdb.QueryResult),
	}

	for _, query := range tsdbQuery.Queries {
		rawQuery := query.Model.Get("rawQuery").MustString()
		if rawQuery == "" {
			continue
		}

		queryResult := &tsdb.QueryResult{Meta: simplejson.New(), RefId: query.RefId}
		result.Results[query.RefId] = queryResult

		rawQuery, err := interpolate(query, tsdbQuery.TimeRange, rawQuery)
		if err != nil {
			queryResult.Error = err
			continue
		}

		log.Info(rawQuery)

		queryResult.Meta.Set("sql", rawQuery)

		rows, err := e.db.Query(rawQuery)
		if err != nil {
			queryResult.Error = err
			continue
		}

		defer rows.Close()

		format := query.Model.Get("format").MustString("time_series")
		fillMissing := query.Model.Get("fill").MustBool(false)
		var fillInterval float64
		fillValue := null.Float{}
		if fillMissing {
			fillInterval = query.Model.Get("fillInterval").MustFloat64() * 1000
			if !query.Model.Get("fillNull").MustBool(false) {
				fillValue.Float64 = query.Model.Get("fillValue").MustFloat64()
				fillValue.Valid = true
			}
		}

		switch format {
		case "time_series":
			resultSeries, rowCount, err := transformToTimeSeries(rows, tsdbQuery.TimeRange, fillMissing, fillInterval, fillValue)

			queryResult.Meta.Set("rowCount", rowCount)
			queryResult.Series = resultSeries

			if err != nil {
				queryResult.Error = err
				continue
			}
		case "table":
			err := errors.New("NotSupported")
			//err := e.transformToTable(query, rows, queryResult, tsdbQuery)
			if err != nil {
				queryResult.Error = err
				continue
			}
		}
	}

	return result, nil
}

func interpolate(query *tsdb.Query, timeRange *tsdb.TimeRange, rawQuery string) (string, error) {
	dateColumnName := query.Model.Get("dateColDataType").MustString()
	dateTimeColumnName := query.Model.Get("dateTimeColDataType").MustString()

	dateSeriesExp1, err := regexp.Compile(regexp.QuoteMeta(dateColumnName) + ` >= toDate\(\d+\)`)
	if err != nil {
		return "", err
	}
	dateTimeSeriesExp1, err := regexp.Compile(regexp.QuoteMeta(dateTimeColumnName) + ` >= toDateTime\(\d+\)`)
	if err != nil {
		return "", err
	}
	dateSeriesExp2, err := regexp.Compile(regexp.QuoteMeta(dateColumnName) + ` BETWEEN toDate\(\d+\) AND toDate\(\d+\)`)
	if err != nil {
		return "", err
	}
	dateTimeSeriesExp2, err := regexp.Compile(regexp.QuoteMeta(dateTimeColumnName) + ` BETWEEN toDateTime\(\d+\) AND toDateTime\(\d+\)`)
	if err != nil {
		return "", err
	}

	rawQuery, err = replaceDateTimeFilter(timeRange, dateSeriesExp1, dateSeriesExp2, dateColumnName, "toDate", rawQuery)
	if err != nil {
		return "", err
	}

	rawQuery, err = replaceDateTimeFilter(timeRange, dateTimeSeriesExp1, dateTimeSeriesExp2, dateTimeColumnName, "toDateTime", rawQuery)
	if err != nil {
		return "", err

	}
	return rawQuery, nil
}

func replaceDateTimeFilter(timeRange *tsdb.TimeRange, re1 *regexp.Regexp, re2 *regexp.Regexp, columnName string, funcName string, query string) (string, error) {
	fromTimeRange := uint64(timeRange.GetFromAsMsEpoch() / 1000)
	var toTimeRange uint64
	if timeRange.To == "now" {
		toTimeRange = 0
	} else {
		toTimeRange = uint64(timeRange.GetToAsMsEpoch() / 1000)
	}

	replaceFunc := func(columnName string, funcName string, groups []string) string {
		if toTimeRange == 0 {
			return columnName + " >= " + funcName + "(" + strconv.FormatUint(fromTimeRange, 10) + ")"
		}
		return columnName + " BETWEEN " + funcName + "(" + strconv.FormatUint(fromTimeRange, 10) + ") AND " + funcName + "(" + strconv.FormatUint(toTimeRange, 10) + ")"
	}
	rawQuery, cnt := replaceAllStringSubmatchFunc(re1, query, func(groups []string) string {
		return replaceFunc(columnName, funcName, groups)
	})
	if cnt > 0 {
		return rawQuery, nil
	}

	rawQuery, cnt = replaceAllStringSubmatchFunc(re2, query, func(groups []string) string {
		return replaceFunc(columnName, funcName, groups)
	})
	if cnt == 0 {
		return "", errors.New("DateTime filter not found")
	}
	return rawQuery, nil
}

func replaceAllStringSubmatchFunc(re *regexp.Regexp, str string, repl func([]string) string) (string, int) {
	result := ""
	lastIndex := 0
	cnt := 0

	for _, v := range re.FindAllSubmatchIndex([]byte(str), -1) {
		groups := []string{}
		for i := 0; i < len(v); i += 2 {
			groups = append(groups, str[v[i]:v[i+1]])
		}

		result += str[lastIndex:v[0]] + repl(groups)
		lastIndex = v[1]
		cnt++
	}

	return result + str[lastIndex:], cnt
}

func transformToTimeSeries(rows *sql.Rows, timeRange *tsdb.TimeRange, fillMissing bool, fillInterval float64, fillValue null.Float) (tsdb.TimeSeriesSlice, int, error) {
	pointsBySeries := make(map[string]*tsdb.TimeSeries)
	seriesByQueryOrder := list.New()
	resultSeries := tsdb.TimeSeriesSlice{}

	columnNames, err := rows.Columns()
	if err != nil {
		return nil, 0, err
	}

	rowLimit := 1000000
	rowCount := 0
	timeIndex := 0

	for rows.Next() {
		var timestamp float64
		var value null.Float
		var metric string

		if rowCount > rowLimit {
			return nil, 0, fmt.Errorf("Query row limit exceeded, limit %d", rowLimit)
		}

		values, err := getTypedRowData(rows)
		if err != nil {
			return nil, 0, err
		}

		// converts column named time to unix timestamp in milliseconds to make
		// native mysql datetime types and epoch dates work in
		// annotation and table queries.
		convertSqlTimeColumnToEpochMs(values, timeIndex)

		switch columnValue := values[timeIndex].(type) {
		case int64:
			timestamp = float64(columnValue)
		case float64:
			timestamp = columnValue
		default:
			return nil, 0, fmt.Errorf("Invalid type for column time/time_sec, must be of type timestamp or unix timestamp, got: %T %v", columnValue, columnValue)
		}

		for i, col := range columnNames {
			if i == timeIndex {
				continue
			}

			switch columnValue := values[i].(type) {
			case *uint64:
				value = null.FloatFrom(float64(*columnValue))
			case *int64:
				value = null.FloatFrom(float64(*columnValue))
			case *float64:
				value = null.FloatFrom(*columnValue)
			case nil:
				value.Valid = false
			default:
				return nil, 0, fmt.Errorf("Value column must have numeric datatype, column: %s type: %T value: %v", col, columnValue, columnValue)
			}

			metric = col

			series, exist := pointsBySeries[metric]
			if !exist {
				series = &tsdb.TimeSeries{Name: metric}
				pointsBySeries[metric] = series
				seriesByQueryOrder.PushBack(metric)
			}

			if fillMissing {
				var intervalStart float64
				if !exist {
					intervalStart = float64(timeRange.MustGetFrom().UnixNano() / 1e6)
				} else {
					intervalStart = series.Points[len(series.Points)-1][1].Float64 + fillInterval
				}

				// align interval start
				intervalStart = math.Floor(intervalStart/fillInterval) * fillInterval

				for i := intervalStart; i < timestamp; i += fillInterval {
					series.Points = append(series.Points, tsdb.TimePoint{fillValue, null.FloatFrom(i)})
					rowCount++
				}
			}

			series.Points = append(series.Points, tsdb.TimePoint{value, null.FloatFrom(timestamp)})
			rowCount++
		}
	}

	for elem := seriesByQueryOrder.Front(); elem != nil; elem = elem.Next() {
		key := elem.Value.(string)
		resultSeries = append(resultSeries, pointsBySeries[key])

		if fillMissing {
			series := pointsBySeries[key]
			// fill in values from last fetched value till interval end
			intervalStart := series.Points[len(series.Points)-1][1].Float64
			intervalEnd := float64(timeRange.MustGetTo().UnixNano() / 1e6)

			// align interval start
			intervalStart = math.Floor(intervalStart/fillInterval) * fillInterval
			for i := intervalStart + fillInterval; i < intervalEnd; i += fillInterval {
				series.Points = append(series.Points, tsdb.TimePoint{fillValue, null.FloatFrom(i)})
				rowCount++
			}
		}
	}

	return resultSeries, rowCount, nil
}

func getTypedRowData(rows *sql.Rows) (tsdb.RowValues, error) {
	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	values := make([]interface{}, len(types))

	for i := range values {
		scanType := types[i].ScanType()
		values[i] = reflect.New(scanType).Interface()
	}

	if err := rows.Scan(values...); err != nil {
		return nil, err
	}

	return values, nil
}

func convertSqlTimeColumnToEpochMs(values tsdb.RowValues, timeIndex int) {
	if timeIndex >= 0 {
		switch value := values[timeIndex].(type) {
		case time.Time:
			values[timeIndex] = epochPrecisionToMs(float64(value.UnixNano()))
		case *time.Time:
			if value != nil {
				values[timeIndex] = epochPrecisionToMs(float64((*value).UnixNano()))
			}
		case int64:
			values[timeIndex] = int64(epochPrecisionToMs(float64(value)))
		case *int64:
			if value != nil {
				values[timeIndex] = int64(epochPrecisionToMs(float64(*value)))
			}
		case uint64:
			values[timeIndex] = int64(epochPrecisionToMs(float64(value)))
		case *uint64:
			if value != nil {
				values[timeIndex] = int64(epochPrecisionToMs(float64(*value)))
			}
		case int32:
			values[timeIndex] = int64(epochPrecisionToMs(float64(value)))
		case *int32:
			if value != nil {
				values[timeIndex] = int64(epochPrecisionToMs(float64(*value)))
			}
		case uint32:
			values[timeIndex] = int64(epochPrecisionToMs(float64(value)))
		case *uint32:
			if value != nil {
				values[timeIndex] = int64(epochPrecisionToMs(float64(*value)))
			}
		case float64:
			values[timeIndex] = epochPrecisionToMs(value)
		case *float64:
			if value != nil {
				values[timeIndex] = epochPrecisionToMs(*value)
			}
		case float32:
			values[timeIndex] = epochPrecisionToMs(float64(value))
		case *float32:
			if value != nil {
				values[timeIndex] = epochPrecisionToMs(float64(*value))
			}
		}
	}
}

func epochPrecisionToMs(value float64) float64 {
	s := strconv.FormatFloat(value, 'e', -1, 64)
	if strings.HasSuffix(s, "e+09") {
		return value * float64(1e3)
	}

	if strings.HasSuffix(s, "e+18") {
		return value / float64(time.Millisecond)
	}

	return value
}
