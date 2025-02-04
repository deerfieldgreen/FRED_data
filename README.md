# FRED data source
https://fred.stlouisfed.org/

Consumption of various data feeds from FRED website. All API based, generally speaking no transformation at this level; only consumption of the data

## Destination
- GitHub ./data directory - committ/push nightly
- GCP CloudStorage Bucket
- CSV format (for now)


## Audit Trail
- In the file audit_trail.csv, you have a few columns to track what's happening
- https://github.com/deerfieldgreen/FRED_data/blob/main/audit_trail.csv
1. the name of the series from FRED
2. the last date, which FRED has shared this datapoint (""FRESHNESS"")
3. The last value, of this data point
4. The last date which the audit code has validated the dataset


```

    # Collect audit information
    last_date = data_df[col_date].max()
    last_value = data_df.loc[data_df[col_date] == last_date, data_ref].values[0]
    audit_data.append({
        "Series Name": data_type,
        "Last Date": last_date,
        "Last Value": last_value,
        "Last Request Datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

```



## Example Series
- DFF is the series name
- this is the Federal Funds Effective Rate
- https://fred.stlouisfed.org/series/DFF
- https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=1320&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=DFF&scale=left&cosd=1954-07-01&coed=2024-11-05&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=3&ost=-99999&oet=99999&mma=0&fml=a&fq=Daily%2C%207-Day&fam=avg&fgst=lin&fgsnd=2020-02-01&line_index=1&transformation=lin&vintage_date=2024-11-07&revision_date=2024-11-07&nd=1954-07-01




# Deployment

We use these commands to deploy the job to GCP CloudRun Jobs
https://console.cloud.google.com/run/jobs/details/us-central1/fred-data-job/executions?hl=en&project=dfg-analytics-insights-prod

### Docker
- **IMPORTANT**: make sure to update requirements.txt file before pushing to docker
  docker build -t gcr.io/dfg-analytics-insights-prod/fred-data .
  docker push gcr.io/dfg-analytics-insights-prod/fred-data
docker run --env-file .env  gcr.io/dfg-analytics-insights-prod/fred-data

## FRED Data Series(es?)
```
grep data_ref settings.yml 
        data_ref: "DFF"
        data_ref: "FRBKCLMCILA"
        data_ref: "UMCSENT"
        data_ref: "IR3TIB01AUM156N"
        data_ref: "IR3TIB01CAM156N"
        data_ref: "IR3TIB01CHM156N"
        data_ref: "IR3TIB01DEM156N"
        data_ref: "IR3TIB01EZM156N"
        data_ref: "IR3TIB01GBM156N"
        data_ref: "IR3TIB01JPM156N"
        data_ref: "IRLTLT01DEM156N"
        data_ref: "IRLTLT01EZM156N"
        data_ref: "USACPALTT01CTGYM"
        data_ref: "EA19CPALTT01GYM"
        data_ref: "DEUCPALTT01CTGYM"
        data_ref: "GBRCPALTT01CTGYM"
        data_ref: "CHECPALTT01CTGYM"
        data_ref: "CPALTT01JPM659N"
        data_ref: "CANCPALTT01CTGYM"
        data_ref: "CPALTT01AUQ659N"
#        data_ref: "SPGLOBALOIL"
#        data_ref: "SPOILGASEP"
```


## Data directories
```
cpi_aus
cpi_can
cpi_chf
cpi_deu
cpi_eur
cpi_gbr
cpi_jpn
cpi_usa
dff
fomc
frb_kc_lmci_monthly
gdp
michigan_csi_monthly
oil_gas_ep
oil_global
rate_aus_3m_bank
rate_chf_3m_bank
rate_cnd_3m_bank
rate_deu_3m_bank
rate_deu_lt_gov
rate_eur_3m_bank
rate_eur_lt_gov
rate_gbp_3m_bank
rate_jpn_3m_bank
total_public_debt
tpd_perc_gdp
```
