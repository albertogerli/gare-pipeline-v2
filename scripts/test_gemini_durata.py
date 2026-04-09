#!/usr/bin/env python3
"""Test Gemini Flash Lite per estrazione durata contratti da testo gare.

Prende 100 CIG senza data_scadenza e con oggetto disponibile,
chiede a Gemini di estrarre la durata, e calcola costi/precisione.
"""

import json
import os
import sys
import time
import pandas as pd
import google.generativeai as genai

# Config
MODEL = "gemini-3.1-flash-lite-preview"
BATCH_SIZE = 100
MAX_RETRIES = 2

# API key
api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
if not api_key:
    print("ERROR: Set GEMINI_API_KEY or GOOGLE_API_KEY")
    sys.exit(1)

genai.configure(api_key=api_key)

# Load dataset
print("Loading dataset...")
df = pd.read_csv(
    "data/output/categorie/gare_unificate.csv.gz",
    compression="gzip",
    low_memory=False,
)
print(f"Total records: {len(df):,}")

# Find CIGs without scadenza but with oggetto
no_scadenza = df[
    df["data_scadenza"].isna()
    & df["oggetto"].notna()
    & (df["oggetto"].str.len() > 20)
    & df["cig"].notna()
].copy()
print(f"Records without data_scadenza (with oggetto): {len(no_scadenza):,}")

# Sample 100
sample = no_scadenza.sample(n=min(BATCH_SIZE, len(no_scadenza)), random_state=42)
print(f"Testing with {len(sample)} CIG\n")

# Prompt template
SYSTEM_PROMPT = """Sei un analista di contratti pubblici italiani. Dato il testo di una gara d'appalto, estrai:
1. durata_giorni: durata del contratto in giorni (converti mesi*30, anni*365). null se non trovata.
2. durata_max_giorni: durata massima inclusi rinnovi/proroghe, se menzionati. null se non trovata.
3. confidence: 0.0-1.0 quanto sei sicuro dell'estrazione.
4. evidence: il frammento di testo da cui hai estratto la durata (max 100 chars).

Regole:
- "triennale"=1095 giorni, "biennale"=730, "quinquennale"=1825, "annuale"=365
- "36 mesi"=1080, "24 mesi"=720, "12 mesi"=360
- Se dice "durata 36 mesi rinnovabile per ulteriori 36 mesi" -> durata_giorni=1080, durata_max_giorni=2160
- NON inventare. Se non c'è info sulla durata, restituisci null.

Rispondi SOLO con JSON valido, nient'altro. Formato:
{"durata_giorni": number|null, "durata_max_giorni": number|null, "confidence": number, "evidence": string|null}"""

# Process
model = genai.GenerativeModel(MODEL)
results = []
total_input_tokens = 0
total_output_tokens = 0
errors = 0
t0 = time.time()

for i, (idx, row) in enumerate(sample.iterrows()):
    cig = row["cig"]
    oggetto = str(row["oggetto"])[:500]  # Limit text length
    categoria = str(row.get("categoria", ""))
    importo = row.get("importo_aggiudicazione", 0)

    user_msg = f"CIG: {cig}\nCategoria: {categoria}\nImporto: €{importo:,.0f}\nTesto gara: {oggetto}"

    for attempt in range(MAX_RETRIES + 1):
        try:
            response = model.generate_content(
                [{"role": "user", "parts": [{"text": SYSTEM_PROMPT + "\n\n" + user_msg}]}],
                generation_config=genai.GenerationConfig(
                    temperature=0.0,
                    max_output_tokens=200,
                    response_mime_type="application/json",
                ),
            )

            # Parse response
            text = response.text.strip()
            parsed = json.loads(text)

            # Track tokens
            if hasattr(response, 'usage_metadata'):
                total_input_tokens += getattr(response.usage_metadata, 'prompt_token_count', 0)
                total_output_tokens += getattr(response.usage_metadata, 'candidates_token_count', 0)

            results.append({
                "cig": cig,
                "oggetto": oggetto[:80],
                "categoria": categoria,
                "durata_giorni": parsed.get("durata_giorni"),
                "durata_max_giorni": parsed.get("durata_max_giorni"),
                "confidence": parsed.get("confidence", 0),
                "evidence": str(parsed.get("evidence", ""))[:100],
                "status": "ok",
            })
            break

        except Exception as e:
            if attempt == MAX_RETRIES:
                errors += 1
                results.append({
                    "cig": cig,
                    "oggetto": oggetto[:80],
                    "categoria": categoria,
                    "durata_giorni": None,
                    "durata_max_giorni": None,
                    "confidence": 0,
                    "evidence": "",
                    "status": f"error: {str(e)[:60]}",
                })
            else:
                time.sleep(1)

    if (i + 1) % 10 == 0:
        elapsed = time.time() - t0
        print(f"  {i+1}/{len(sample)} processed ({elapsed:.1f}s, {errors} errors)")

elapsed = time.time() - t0
print(f"\n{'='*60}")
print(f"RESULTS: {len(sample)} CIG in {elapsed:.1f}s ({elapsed/len(sample):.2f}s/CIG)")
print(f"{'='*60}")

# Analysis
df_res = pd.DataFrame(results)
ok = df_res[df_res["status"] == "ok"]
with_durata = ok[ok["durata_giorni"].notna()]
high_conf = with_durata[with_durata["confidence"] >= 0.7]

print(f"\nSuccess rate: {len(ok)}/{len(df_res)} ({len(ok)/len(df_res)*100:.1f}%)")
print(f"Durata found: {len(with_durata)}/{len(ok)} ({len(with_durata)/max(1,len(ok))*100:.1f}%)")
print(f"High confidence (>=0.7): {len(high_conf)}/{len(with_durata)} ({len(high_conf)/max(1,len(with_durata))*100:.1f}%)")

if len(with_durata) > 0:
    print(f"\nDurata distribution (giorni):")
    print(f"  Mean: {with_durata['durata_giorni'].mean():.0f}")
    print(f"  Median: {with_durata['durata_giorni'].median():.0f}")
    print(f"  Min: {with_durata['durata_giorni'].min():.0f}")
    print(f"  Max: {with_durata['durata_giorni'].max():.0f}")

# Cost estimate
print(f"\nToken usage:")
print(f"  Input: {total_input_tokens:,}")
print(f"  Output: {total_output_tokens:,}")
print(f"  Total: {total_input_tokens + total_output_tokens:,}")

# Gemini Flash Lite pricing (as of 2025):
# Input: $0.01/M tokens, Output: $0.01/M tokens (approx - very cheap)
cost_in = total_input_tokens * 0.00001 / 1000  # Placeholder
cost_out = total_output_tokens * 0.00001 / 1000
cost_100 = cost_in + cost_out

# Scale to full dataset
n_missing = len(no_scadenza)
scale = n_missing / len(sample) if len(sample) > 0 else 0
cost_all = cost_100 * scale
tokens_all_in = total_input_tokens * scale
tokens_all_out = total_output_tokens * scale

print(f"\n{'='*60}")
print(f"COST ESTIMATE")
print(f"{'='*60}")
print(f"Test (100 CIG):    {total_input_tokens + total_output_tokens:>10,} tokens  ~${cost_100:.4f}")
print(f"Full ({n_missing:,} CIG): {int(tokens_all_in + tokens_all_out):>10,} tokens  ~${cost_all:.2f}")
print(f"Time estimate:     {elapsed/len(sample) * n_missing / 60:.0f} min")

# Save results
out_path = "data/output/test_gemini_durata_100.csv"
df_res.to_csv(out_path, index=False)
print(f"\nResults saved to {out_path}")

# Show some examples
print(f"\nSample results (high confidence):")
for _, r in high_conf.head(10).iterrows():
    print(f"  CIG {r['cig']}: {r['durata_giorni']:.0f}d (max:{r.get('durata_max_giorni','N/A')}) conf={r['confidence']:.1f} | {r['evidence'][:60]}")
