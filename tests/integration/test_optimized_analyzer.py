#!/usr/bin/env python
"""
Test del sistema ottimizzato a due stadi
Confronta performance e costi tra sistema originale e ottimizzato
"""

import sys
import time
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from src.analyzers.gazzetta_analyzer_optimized import (
    GazzettaAnalyzerOptimized, 
    stage1_quick_filter,
    stage2_detailed_extraction,
    Config
)
import pandas as pd

# Test samples
TEST_SAMPLES = [
    {
        "testo": """ILLUMINAZIONE PUBBLICA - Comune di Milano
        Affidamento del servizio di gestione e manutenzione ordinaria e straordinaria 
        degli impianti di illuminazione pubblica del territorio comunale.
        Importo: €2.500.000. CIG: ABC123DEF. Aggiudicatario: Energy Solutions SpA"""
    },
    {
        "testo": """FORNITURA CANCELLERIA - Azienda Sanitaria Locale
        Procedura aperta per la fornitura di materiale di cancelleria per uffici.
        Lotto 1: Carta A4 - €50.000
        Lotto 2: Penne e matite - €10.000
        Non rilevante per infrastrutture pubbliche."""
    },
    {
        "testo": """RIQUALIFICAZIONE ENERGETICA EDIFICI SCOLASTICI
        Lavori di efficientamento energetico presso:
        - Scuola Elementare Via Roma: sostituzione infissi e cappotto termico
        - Scuola Media Galilei: impianto fotovoltaico 50kW
        - Liceo Scientifico: LED e sistema domotica
        Importo totale: €1.800.000 suddiviso in 3 lotti"""
    }
]

def test_stage1():
    """Test filtro rapido Stage 1"""
    print("\n" + "="*70)
    print("🧪 TEST STAGE 1: FILTRO RAPIDO (GPT-4o-mini)")
    print("="*70)
    
    for i, sample in enumerate(TEST_SAMPLES, 1):
        print(f"\n📝 Sample {i}:")
        print(f"   Testo: {sample['testo'][:100]}...")
        
        start = time.time()
        result = stage1_quick_filter(sample['testo'])
        elapsed = time.time() - start
        
        print(f"   ✓ Rilevante: {result.is_relevant}")
        print(f"   ✓ Confidenza: {result.confidence:.0%}")
        print(f"   ✓ Categoria: {result.category}")
        print(f"   ✓ Lotti stimati: {result.estimated_lots}")
        print(f"   ✓ Motivo: {result.reason}")
        print(f"   ⏱️  Tempo: {elapsed:.2f}s")

def test_full_pipeline():
    """Test pipeline completo con confronto"""
    print("\n" + "="*70)
    print("🧪 TEST PIPELINE COMPLETO")
    print("="*70)
    
    # Crea DataFrame di test
    df_test = pd.DataFrame(TEST_SAMPLES)
    df_test['source'] = 'test'
    df_test['date'] = '2024-01-01'
    
    # Salva file temporaneo
    test_input = Path(Config.TEMP_DIR) / "test_input.xlsx"
    test_input.parent.mkdir(exist_ok=True)
    df_test.to_excel(test_input, index=False)
    
    print(f"\n📊 Records di test: {len(df_test)}")
    
    # Stima token
    estimate = GazzettaAnalyzerOptimized.estimate_tokens(len(df_test))
    
    print("\n💰 STIMA COSTI:")
    print(f"   Sistema originale (solo o3):")
    print(f"      Token: {len(df_test) * 2500:,}")
    print(f"      Costo: ${len(df_test) * 2500 / 1_000_000 * 5:.2f}")
    
    print(f"\n   Sistema ottimizzato (2-stage):")
    print(f"      Stage 1: {estimate['stage1_tokens']:,} token")
    print(f"      Stage 2: {estimate['stage2_tokens']:,} token")
    print(f"      Totale: {estimate['total_tokens']:,} token")
    print(f"      Costo: ${estimate['total_cost']:.2f}")
    print(f"      Risparmio: {estimate['savings_percent']:.1f}%")
    
    # Esegui test
    response = input("\n🚦 Eseguire test completo? (s/n): ")
    if response.lower() == 's':
        print("\n⚙️  Esecuzione pipeline ottimizzato...")
        
        start = time.time()
        GazzettaAnalyzerOptimized.run(
            input_file="test_input.xlsx",
            output_file="test_output_optimized.xlsx"
        )
        elapsed = time.time() - start
        
        # Verifica risultati
        output_path = Path(Config.TEMP_DIR) / "test_output_optimized.xlsx"
        if output_path.exists():
            df_result = pd.read_excel(output_path)
            
            print("\n✅ TEST COMPLETATO")
            print(f"   Input records: {len(df_test)}")
            print(f"   Output lotti: {len(df_result)}")
            print(f"   Tempo: {elapsed:.2f}s")
            
            # Mostra risultati
            print("\n📋 RISULTATI:")
            for _, row in df_result.iterrows():
                print(f"\n   Lotto: {row['Lotto']}")
                print(f"   Categoria: {row['Categoria']}")
                print(f"   Quick Category: {row['QuickCategory']}")
                print(f"   Confidence: {row['FilterConfidence']:.0%}")
                print(f"   Oggetto: {row['Oggetto'][:50]}...")

def compare_token_usage():
    """Confronta uso token per dataset reale"""
    print("\n" + "="*70)
    print("📊 CONFRONTO TOKEN USAGE (34,100 records)")
    print("="*70)
    
    records = 34_100
    
    # Sistema originale (solo o3)
    original_tokens = records * 2500
    original_cost_input = (original_tokens * 0.8) / 1_000_000 * 3
    original_cost_output = (original_tokens * 0.2) / 1_000_000 * 15
    original_total = original_cost_input + original_cost_output
    
    # Sistema ottimizzato
    estimate = GazzettaAnalyzerOptimized.estimate_tokens(records)
    
    print(f"\n📈 SISTEMA ORIGINALE (solo o3-mini):")
    print(f"   Records: {records:,}")
    print(f"   Token totali: {original_tokens:,}")
    print(f"   Costo: ${original_total:.2f}")
    print(f"   Tempo stimato: ~{records * 2 / 3600:.1f} ore")
    
    print(f"\n🚀 SISTEMA OTTIMIZZATO (2-stage):")
    print(f"   Stage 1 (tutti): {estimate['stage1_records']:,} records")
    print(f"   Stage 2 (30%): {estimate['stage2_records']:,} records")
    print(f"   Token totali: {estimate['total_tokens']:,}")
    print(f"   Costo: ${estimate['total_cost']:.2f}")
    print(f"   Tempo stimato: ~{records * 0.8 / 3600:.1f} ore")
    
    print(f"\n💎 RISPARMIO:")
    print(f"   Token: -{100 * (1 - estimate['total_tokens']/original_tokens):.1f}%")
    print(f"   Costo: -${original_total - estimate['total_cost']:.2f} ({100 * (1 - estimate['total_cost']/original_total):.1f}%)")
    print(f"   Tempo: -{100 * (1 - 0.8/2):.0f}%")
    
    # ROI Analysis
    print(f"\n📊 ANALISI ROI:")
    print(f"   Break-even: dopo ~{int(100 / (original_total - estimate['total_cost']))} batch")
    print(f"   Risparmio mensile (4 batch): ${4 * (original_total - estimate['total_cost']):.2f}")
    print(f"   Risparmio annuale: ${48 * (original_total - estimate['total_cost']):.2f}")

def main():
    """Menu principale test"""
    
    print("\n" + "="*70)
    print("🧪 TEST SISTEMA OTTIMIZZATO GAZZETTA ANALYZER")
    print("="*70)
    
    # Verifica configurazione
    if not Config.OPENAI_API_KEY or Config.OPENAI_API_KEY.startswith("your-"):
        print("\n❌ OPENAI_API_KEY non configurata!")
        print("   Copia .env.example in .env e inserisci la tua chiave API")
        return
    
    print("\n✅ Configurazione caricata da .env")
    print(f"   Two-Stage: {'✅' if Config.USE_TWO_STAGE else '❌'}")
    print(f"   Mini Model: {Config.MINI_MODEL}")
    print(f"   O3 Model: {Config.O3_MODEL}")
    
    while True:
        print("\n📋 OPZIONI TEST:")
        print("   1. Test Stage 1 (filtro rapido)")
        print("   2. Test pipeline completo")
        print("   3. Confronto token usage")
        print("   4. Esci")
        
        choice = input("\nScelta (1-4): ")
        
        if choice == '1':
            test_stage1()
        elif choice == '2':
            test_full_pipeline()
        elif choice == '3':
            compare_token_usage()
        elif choice == '4':
            print("\n👋 Arrivederci!")
            break
        else:
            print("❌ Scelta non valida")

if __name__ == "__main__":
    main()