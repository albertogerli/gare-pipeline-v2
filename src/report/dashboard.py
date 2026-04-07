"""
Genera una dashboard HTML interattiva con Chart.js su output finale.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from config.settings import config


def generate_dashboard() -> Path:
    output_dir = config.OUTPUT_DIR
    dashboard_path = output_dir / "dashboard.html"

    # Scegli sorgente: Parquet > Excel > CSV
    gare_path = config.get_file_path(config.GARE, "output")
    if not gare_path.exists() and gare_path.with_suffix('.csv').exists():
        gare_path = gare_path.with_suffix('.csv')
    if not gare_path.exists() and gare_path.with_suffix('.parquet').exists():
        gare_path = gare_path.with_suffix('.parquet')

    if not gare_path.exists():
        # crea una pagina vuota informativa
        dashboard_path.write_text("<html><body><h2>Nessun dato trovato per generare la dashboard.</h2></body></html>", encoding='utf-8')
        return dashboard_path

    # Carica dati
    if gare_path.suffix == '.parquet':
        df = pd.read_parquet(gare_path)
    elif gare_path.suffix == '.csv':
        df = pd.read_csv(gare_path)
    else:
        df = pd.read_excel(gare_path)

    # KPI
    totale = int(len(df))
    importo_tot = float(pd.to_numeric(df.get('ImportoAggiudicazione'), errors='coerce').sum()) if 'ImportoAggiudicazione' in df.columns else 0.0
    per_anno = (
        df.assign(Anno=pd.to_datetime(df.get('DataAggiudicazione'), errors='coerce').dt.year)
          .dropna(subset=['Anno'])
          .groupby('Anno')
          .size()
          .to_dict()
    ) if 'DataAggiudicazione' in df.columns else {}
    per_categoria = df['Categoria'].value_counts().head(10).to_dict() if 'Categoria' in df.columns else {}
    per_regione = df['Regione'].value_counts().head(15).to_dict() if 'Regione' in df.columns else {}

    # Analisi Sconti
    sconto_perc = None
    if 'ScontoPerc' in df.columns:
        sconto_perc = pd.to_numeric(df['ScontoPerc'], errors='coerce')
    elif 'Sconto' in df.columns:
        s_raw = (
            df['Sconto'].astype(str)
            .str.replace('%', '', regex=False)
            .str.replace(',', '.', regex=False)
        )
        s_num = pd.to_numeric(s_raw, errors='coerce')
        sconto_perc = s_num / 100.0

    sconto_stats = {}
    sconto_hist = {}
    sconto_per_anno = {}
    sconto_per_categoria = {}
    sconto_scatter = { 'x_importo': [], 'y_sconto': [] }
    top_sconti = []

    if sconto_perc is not None:
        s = sconto_perc[(sconto_perc >= 0) & (sconto_perc <= 1)].dropna()
        avail = int(s.notna().sum())
        rate = float(avail) / max(1, totale)
        mean = float(s.mean() * 100) if len(s) else 0.0
        median = float(s.median() * 100) if len(s) else 0.0
        sconto_stats = {
            'disponibili': avail,
            'tasso_disponibilita': rate,
            'media_pct': mean,
            'mediana_pct': median,
        }

        # Istogramma (bin in %)
        bins = [0,5,10,15,20,25,30,35,40,50,100]
        labels = [f"{bins[i]}–{bins[i+1]}%" for i in range(len(bins)-1)]
        cut = pd.cut((s*100), bins=bins, labels=labels, include_lowest=True, right=True)
        sconto_hist = cut.value_counts().sort_index().to_dict()

        # Per Anno
        if 'DataAggiudicazione' in df.columns:
            anno = pd.to_datetime(df['DataAggiudicazione'], errors='coerce').dt.year
            sconto_per_anno = (
                pd.DataFrame({'Anno': anno, 'ScontoPerc': sconto_perc})
                  .dropna()
                  .groupby('Anno')['ScontoPerc']
                  .mean()
                  .mul(100)
                  .round(2)
                  .to_dict()
            )
        # Per Categoria
        if 'Categoria' in df.columns:
            sconto_per_categoria = (
                pd.DataFrame({'Categoria': df['Categoria'], 'ScontoPerc': sconto_perc})
                  .dropna()
                  .groupby('Categoria')['ScontoPerc']
                  .mean()
                  .mul(100)
                  .round(2)
                  .sort_values(ascending=False)
                  .head(15)
                  .to_dict()
            )
        # Scatter (campione) Importo vs Sconto
        if 'ImportoAggiudicazione' in df.columns:
            imp = pd.to_numeric(df['ImportoAggiudicazione'], errors='coerce')
            scatter_df = pd.DataFrame({'imp': imp, 's': sconto_perc}).dropna()
            if len(scatter_df) > 2000:
                scatter_df = scatter_df.sample(2000, random_state=42)
            sconto_scatter['x_importo'] = [float(x) for x in scatter_df['imp'].tolist()]
            sconto_scatter['y_sconto'] = [float(y*100) for y in scatter_df['s'].tolist()]
        
        # Top sconti (tabella)
        cols = [c for c in ['CIG','Oggetto','Comune','Regione','Aggiudicatario','ImportoAggiudicazione','DataAggiudicazione'] if c in df.columns]
        if cols:
            df_top = pd.DataFrame({'ScontoPerc': s}).join(df[cols])
            df_top = df_top.sort_values('ScontoPerc', ascending=False).head(50)
            for _, r in df_top.iterrows():
                top_sconti.append({
                    'CIG': str(r.get('CIG', '')),
                    'Oggetto': str(r.get('Oggetto', ''))[:140],
                    'Comune': str(r.get('Comune', '')),
                    'Regione': str(r.get('Regione', '')),
                    'Aggiudicatario': str(r.get('Aggiudicatario', ''))[:80],
                    'ImportoAggiudicazione': float(r.get('ImportoAggiudicazione', 0) or 0),
                    'DataAggiudicazione': str(r.get('DataAggiudicazione', '')),
                    'ScontoPerc': float(r['ScontoPerc']*100)
                })

    # Minor Prezzo (conteggio e per-anno)
    minor_prezzo_count = 0
    minor_prezzo_per_anno = {}
    if 'MinorPrezzo' in df.columns:
        minor_df = df[df['MinorPrezzo'] == True]
        minor_prezzo_count = int(len(minor_df))
        if 'DataAggiudicazione' in minor_df.columns:
            anni = pd.to_datetime(minor_df['DataAggiudicazione'], errors='coerce').dt.year
            minor_prezzo_per_anno = anni.dropna().value_counts().sort_index().astype(int).to_dict()

    data = {
        'kpi': {
            'totale': totale,
            'importo_tot': importo_tot,
            'sconto': sconto_stats,
        },
        'serie': {
            'per_anno': per_anno,
            'per_categoria': per_categoria,
            'per_regione': per_regione,
            'sconto_hist': sconto_hist,
            'sconto_per_anno': sconto_per_anno,
            'sconto_per_categoria': sconto_per_categoria,
            'sconto_scatter': sconto_scatter,
            'top_sconti': top_sconti,
            'minor_prezzo_count': minor_prezzo_count,
            'minor_prezzo_per_anno': minor_prezzo_per_anno,
        }
    }

    # HTML semplice con Chart.js
    html = f"""
<!doctype html>
<html lang=it>
<head>
  <meta charset=utf-8>
  <meta name=viewport content="width=device-width, initial-scale=1">
  <title>Dashboard Gare</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    body {{ font-family: Inter, system-ui, Arial; margin: 24px; background: #0b1220; color:#e6edf3; }}
    .kpis {{ display:flex; gap:24px; flex-wrap:wrap; margin-bottom:24px; }}
    .card {{ background:#111a2b; padding:16px 20px; border-radius:12px; min-width:220px; box-shadow:0 2px 10px rgba(0,0,0,0.3); }}
    h1 {{ margin:0 0 12px 0; font-size:22px; }}
    .grid {{ display:grid; grid-template-columns: repeat(auto-fit, minmax(320px,1fr)); gap:24px; }}
    canvas {{ background:#0f1726; border-radius:12px; padding:8px; }}
    .muted {{ color:#8a94a6; font-size:12px; }}
  </style>
</head>
<body>
  <h1>Dashboard Gare</h1>
  <div class="kpis">
    <div class="card"><div class="muted">Record totali</div><div style="font-size:28px; font-weight:600">{data['kpi']['totale']:,}</div></div>
    <div class="card"><div class="muted">Importo totale aggiudicazioni</div><div style="font-size:28px; font-weight:600">€{data['kpi']['importo_tot']:,.2f}</div></div>
    <div class="card"><div class="muted">Sconto medio</div><div style="font-size:28px; font-weight:600">{data['kpi']['sconto'].get('media_pct', 0):.2f}%</div><div class="muted">mediana {data['kpi']['sconto'].get('mediana_pct', 0):.2f}% • copertura {data['kpi']['sconto'].get('tasso_disponibilita', 0):.0%}</div></div>
    <div class="card"><div class="muted">Gare a Minor Prezzo</div><div style="font-size:28px; font-weight:600">{data['serie']['minor_prezzo_count']:,}</div></div>
  </div>
  <div class="grid">
    <div>
      <h3>Distribuzione per Anno</h3>
      <canvas id="chartAnno"></canvas>
    </div>
    <div>
      <h3>Top Categorie</h3>
      <canvas id="chartCategoria"></canvas>
    </div>
    <div>
      <h3>Top Regioni</h3>
      <canvas id="chartRegione"></canvas>
    </div>
    <div>
      <h3>Gare a Minor Prezzo per Anno</h3>
      <canvas id="chartMinorPrezzoAnno"></canvas>
    </div>
    <div>
      <h3>Distribuzione Sconti (%)</h3>
      <canvas id="chartScontoHist"></canvas>
    </div>
    <div>
      <h3>Sconto medio per Anno (%)</h3>
      <canvas id="chartScontoAnno"></canvas>
    </div>
    <div>
      <h3>Sconto medio per Categoria (%)</h3>
      <canvas id="chartScontoCategoria"></canvas>
    </div>
    <div>
      <h3>Scatter: Sconto (%) vs Importo</h3>
      <canvas id="chartScatter"></canvas>
    </div>
    <div>
      <h3>Top 50 Sconti (dettaglio gare)</h3>
      <div class="card" style="overflow:auto; max-height:480px">
        <table style="width:100%; border-collapse:collapse; font-size:12px">
          <thead>
            <tr>
              <th style="text-align:left; padding:6px">Sconto %</th>
              <th style="text-align:left; padding:6px">CIG</th>
              <th style="text-align:left; padding:6px">Oggetto</th>
              <th style="text-align:left; padding:6px">Aggiudicatario</th>
              <th style="text-align:left; padding:6px">Comune</th>
              <th style="text-align:left; padding:6px">Regione</th>
              <th style="text-align:left; padding:6px">Importo</th>
              <th style="text-align:left; padding:6px">Data</th>
            </tr>
          </thead>
          <tbody id="tableTopSconti"></tbody>
        </table>
      </div>
    </div>
  </div>
  <script>
    const dataSerie = {json.dumps(data)};
    const fmt = (n) => n.toLocaleString('it-IT');

    new Chart(document.getElementById('chartAnno'), {{
      type: 'bar',
      data: {{
        labels: Object.keys(dataSerie.serie.per_anno),
        datasets: [{{label:'Record', data:Object.values(dataSerie.serie.per_anno), backgroundColor:'#3b82f6'}}]
      }},
      options: {{ responsive:true, plugins:{{ legend:{{ display:false }} }} }}
    }});

    new Chart(document.getElementById('chartCategoria'), {{
      type: 'bar',
      data: {{
        labels: Object.keys(dataSerie.serie.per_categoria),
        datasets: [{{label:'Record', data:Object.values(dataSerie.serie.per_categoria), backgroundColor:'#22c55e'}}]
      }},
      options: {{ indexAxis:'y', responsive:true, plugins:{{ legend:{{ display:false }} }} }}
    }});

    new Chart(document.getElementById('chartRegione'), {{
      type: 'bar',
      data: {{
        labels: Object.keys(dataSerie.serie.per_regione),
        datasets: [{{label:'Record', data:Object.values(dataSerie.serie.per_regione), backgroundColor:'#a855f7'}}]
      }},
      options: {{ indexAxis:'y', responsive:true, plugins:{{ legend:{{ display:false }} }} }}
    }});

    // Sconti: Istogramma
    new Chart(document.getElementById('chartScontoHist'), {{
      type: 'bar',
      data: {{
        labels: Object.keys(dataSerie.serie.sconto_hist),
        datasets: [{{label:'Record', data:Object.values(dataSerie.serie.sconto_hist), backgroundColor:'#f59e0b'}}]
      }},
      options: {{ responsive:true, plugins:{{ legend:{{ display:false }} }} }}
    }});

    // Sconti: per Anno
    new Chart(document.getElementById('chartScontoAnno'), {{
      type: 'line',
      data: {{
        labels: Object.keys(dataSerie.serie.sconto_per_anno),
        datasets: [{{label:'Sconto medio %', data:Object.values(dataSerie.serie.sconto_per_anno), borderColor:'#38bdf8', fill:false}}]
      }},
      options: {{ responsive:true, plugins:{{ legend:{{ display:true }} }} }}
    }});

    // Sconti: per Categoria
    new Chart(document.getElementById('chartScontoCategoria'), {{
      type: 'bar',
      data: {{
        labels: Object.keys(dataSerie.serie.sconto_per_categoria),
        datasets: [{{label:'Sconto medio %', data:Object.values(dataSerie.serie.sconto_per_categoria), backgroundColor:'#10b981'}}]
      }},
      options: {{ indexAxis:'y', responsive:true, plugins:{{ legend:{{ display:false }} }} }}
    }});

    // Scatter Sconto vs Importo
    const points = (dataSerie.serie.sconto_scatter.x_importo || []).map((x, i) => ({{x: x, y: dataSerie.serie.sconto_scatter.y_sconto[i]}}));
    new Chart(document.getElementById('chartScatter'), {{
      type: 'scatter',
      data: {{ datasets: [{{ label: 'Gare', data: points, pointRadius: 2, backgroundColor: 'rgba(255,99,132,0.6)'}}] }},
      options: {{ responsive:true, scales:{{ x:{{ title:{{ display:true, text:'Importo (€)'}} }}, y:{{ title:{{ display:true, text:'Sconto (%)'}} }} }} }}
    }});

    // Minor Prezzo per Anno
    new Chart(document.getElementById('chartMinorPrezzoAnno'), {{
      type: 'bar',
      data: {{
        labels: Object.keys(dataSerie.serie.minor_prezzo_per_anno || {{}}),
        datasets: [{{label:'Gare a minor prezzo', data:Object.values(dataSerie.serie.minor_prezzo_per_anno || {{}}), backgroundColor:'#fde047'}}]
      }},
      options: {{ responsive:true, plugins:{{ legend:{{ display:false }} }} }}
    }});

    // Tabella Top Sconti
    const tbody = document.getElementById('tableTopSconti');
    if (tbody) {{
      tbody.innerHTML = (dataSerie.serie.top_sconti || []).map(r => `
        <tr>
          <td style="padding:6px">${(r.ScontoPerc||0).toFixed(2)}%</td>
          <td style="padding:6px; font-family:ui-monospace">${r.CIG||''}</td>
          <td style="padding:6px">${r.Oggetto||''}</td>
          <td style="padding:6px">${r.Aggiudicatario||''}</td>
          <td style="padding:6px">${r.Comune||''}</td>
          <td style="padding:6px">${r.Regione||''}</td>
          <td style="padding:6px">€${(r.ImportoAggiudicazione||0).toLocaleString('it-IT')}</td>
          <td style="padding:6px">${r.DataAggiudicazione||''}</td>
        </tr>
      `).join('');
    }}
  </script>
</body>
</html>
"""

    dashboard_path.write_text(html, encoding='utf-8')
    return dashboard_path


if __name__ == "__main__":
    p = generate_dashboard()
    print(f"Dashboard generata: {p}")


