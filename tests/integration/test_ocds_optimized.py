#!/usr/bin/env python
"""
Test rapido OCDS Analyzer Ottimizzato
Verifica che il sistema funzioni correttamente prima di processare tutti i dati
"""

import sys
import json
from pathlib import Path

sys.path.append(str(Path(__file__).parent))

from src.analyzers.ocds_analyzer_optimized import (
    OCDSAnalyzerOptimized,
    stage1_ocds_filter,
    stage2_ocds_extraction,
    Config
)

def test_with_sample():
    """Test con un sample OCDS reale"""
    
    print("\n" + "="*70)
    print("🧪 TEST OCDS ANALYZER OTTIMIZZATO")
    print("="*70)
    
    # Trova primo file OCDS valido
    ocds_dir = Path("data/ocds")
    ocds_files = list(ocds_dir.glob("*.json"))
    ocds_files = [f for f in ocds_files if f.name != "ocds_example.json"]
    
    if not ocds_files:
        print("❌ Nessun file OCDS trovato per test")
        return
    
    test_file = ocds_files[0]
    print(f"\n📁 File test: {test_file.name}")
    
    # Carica primi releases
    with open(test_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    releases = data.get('releases', [])[:5]  # Test su primi 5
    print(f"📊 Releases di test: {len(releases)}")
    
    if not releases:
        print("❌ Nessun release nel file")
        return
    
    # Test Stage 1
    print("\n" + "-"*70)
    print("🔍 TEST STAGE 1: Filtro Rapido")
    print("-"*70)
    
    relevant_count = 0
    for i, release in enumerate(releases, 1):
        print(f"\n📝 Release {i}:")
        tender_title = release.get('tender', {}).get('title', 'N/A')[:80]
        print(f"   Titolo: {tender_title}...")
        
        # Stage 1 filter
        filter_result = stage1_ocds_filter(release)
        
        print(f"   ✓ Rilevante: {filter_result.is_relevant}")
        print(f"   ✓ Confidenza: {filter_result.confidence:.0%}")
        print(f"   ✓ Tipo: {filter_result.tender_type}")
        print(f"   ✓ Categoria: {filter_result.category}")
        print(f"   ✓ Importo: {filter_result.amount_range}")
        print(f"   ✓ Motivo: {filter_result.reason}")
        
        if filter_result.is_relevant:
            relevant_count += 1
    
    print(f"\n📊 Rilevanti: {relevant_count}/{len(releases)}")
    
    # Test Stage 2 su primo rilevante
    print("\n" + "-"*70)
    print("🎯 TEST STAGE 2: Estrazione Dettagliata")
    print("-"*70)
    
    for release in releases:
        filter_result = stage1_ocds_filter(release)
        if filter_result.is_relevant:
            print("\n📋 Estrazione completa dal primo release rilevante...")
            
            lotto = stage2_ocds_extraction(release)
            
            print(f"\n✅ DATI ESTRATTI:")
            print(f"   OCID: {lotto.ocid}")
            print(f"   Titolo: {lotto.tender_title[:80]}...")
            print(f"   Categoria: {lotto.category}")
            print(f"   Metodo: {lotto.procurement_method}")
            print(f"   Valore Gara: €{lotto.tender_value}")
            print(f"   Valore Aggiudicazione: €{lotto.award_value}")
            print(f"   Acquirente: {lotto.buyer_name}")
            print(f"   Fornitore: {lotto.supplier_name}")
            print(f"   Data Pubblicazione: {lotto.tender_date_published}")
            print(f"   CIG: {lotto.cig or 'N/A'}")
            print(f"   Stato: {lotto.tender_status}")
            
            break
    
    # Stima costi
    print("\n" + "-"*70)
    print("💰 STIMA COSTI PER TUTTI I FILE")
    print("-"*70)
    
    total_releases = OCDSAnalyzerOptimized.count_total_releases()
    estimate = OCDSAnalyzerOptimized.estimate_tokens(total_releases)
    
    print(f"\n📊 Releases totali: {total_releases:,}")
    print(f"\n💵 Sistema Two-Stage:")
    print(f"   Stage 1: {estimate['stage1_releases']:,} releases")
    print(f"   Stage 2: ~{estimate['stage2_releases']:,} releases (30%)")
    print(f"   Token: {estimate['total_tokens']:,}")
    print(f"   Costo: ${estimate['total_cost']:.2f}")
    
    print(f"\n💵 Sistema Originale (solo o3):")
    print(f"   Token: {total_releases * 3500:,}")
    print(f"   Costo: ${estimate['original_cost']:.2f}")
    
    print(f"\n✨ RISPARMIO:")
    print(f"   Importo: ${estimate['savings_amount']:.2f}")
    print(f"   Percentuale: {estimate['savings_percent']:.1f}%")
    
    print("\n" + "="*70)
    print("✅ TEST COMPLETATO")
    print("="*70)

def main():
    # Check API
    if not Config.OPENAI_API_KEY or Config.OPENAI_API_KEY.startswith("your-"):
        print("❌ OPENAI_API_KEY non configurata in .env")
        return
    
    print("✅ API Key configurata")
    
    # Run test
    test_with_sample()
    
    print("\n🚀 Per eseguire l'analisi completa:")
    print("   python -m src.analyzers.ocds_analyzer_optimized")

if __name__ == "__main__":
    main()